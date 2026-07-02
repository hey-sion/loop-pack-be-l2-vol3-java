# Loopers Commerce — 대규모 트래픽 대응 이커머스 백엔드

Spring Boot 멀티모듈 기반 이커머스 백엔드입니다. 상품/주문/결제/쿠폰 같은 커머스 도메인을 DDD로 설계하고, 여기에 **트래픽 급증 상황을 견디기 위한 인프라 패턴**(대기열, 서킷 브레이커, 캐싱, 이벤트 기반 비동기 처리, 실시간 랭킹, 배치 집계)을 단계적으로 얹어가며 구현했습니다.

부트캠프 과제를 Round 단위로 진행하며, 매 라운드마다 **"어떤 선택지가 있었고 왜 이 방식을 골랐는가"**를 문서로 남기는 방식으로 작업했습니다.

---

## 기술 스택

| 구분 | 사용 기술 |
|------|-----------|
| Language / Framework | Java 21, Spring Boot 3.4, Spring Batch, Spring Cloud OpenFeign |
| 데이터 | MySQL, JPA, QueryDSL |
| 캐시 / 인메모리 | Redis (Master-Replica, Lettuce) |
| 메시징 | Apache Kafka |
| 회복력 | Resilience4j (Circuit Breaker, Retry) |
| 빌드 / 테스트 | Gradle (Kotlin DSL), JUnit5, TestContainers, WireMock, EmbeddedKafka |

---

## 아키텍처

**멀티모듈 + Clean Architecture / DDD**

```
apps/
  ├─ commerce-api       # REST API 서버 (Producer)
  ├─ commerce-batch     # Spring Batch (랭킹 집계)
  └─ commerce-streamer  # Kafka Consumer (집계/랭킹 반영)
modules/  jpa · redis · kafka · event-contract
supports/ jackson · logging · monitoring
```

`commerce-api`는 `interfaces → application → domain → infrastructure` 레이어로 분리하고, 도메인 레이어에는 외부 상태에 의존하지 않는 순수 규칙만 남겼습니다. Repository를 통한 유스케이스 조율은 `application`에, 여러 도메인을 엮는 오케스트레이션은 Facade에 배치했습니다.

---

## 구현 여정 (Round 1 → 10)

| Round | 주제 | 핵심 |
|-------|------|------|
| 1–3 | 커머스 도메인 구축 | 브랜드/상품/좋아요/주문 도메인, 헤더 기반 인증(인터셉터 + ArgumentResolver), E2E 테스트 |
| 4 | 주문 정합성 & 동시성 | 쿠폰(정액/정률) 할인, 재고·쿠폰의 원자적 UPDATE, 동시성 테스트 |
| 5 | 조회 성능 최적화 | 복합 인덱스 설계, Redis 캐시 (응답시간 770ms → 10ms) |
| 6 | 결제 & 외부 연동 회복력 | PG FeignClient 연동, **Circuit Breaker / Retry / Fallback**, 보상 트랜잭션 |
| 7 | 이벤트 기반 아키텍처 | Kafka, **Transactional Outbox**, Consumer 멱등성, 선착순 쿠폰(Redis 컷오프) |
| 8 | 주문 대기열 | **Redis Sorted Set 대기열 + 토큰 발급 기반 유입 제어** |
| 9 | 실시간 랭킹 | Kafka 이벤트 → Redis ZSET `ZINCRBY` 가중치 스코어링 |
| 10 | 랭킹 배치 집계 | Spring Batch Chunk Processing, 주간/월간 롤링 윈도우 집계 |

---

## 핵심 기술 구현

### 1. Redis 대기열 기반 유입 제어 (Round 8)

블랙 프라이데이처럼 주문이 폭증할 때, 보호해야 할 진짜 대상은 톰캣 스레드풀이 아니라 **그 뒤의 DB와 PG**입니다. 톰캣 큐는 요청을 쌓아주기만 할 뿐 유저에게 피드백이 없어 "새로고침 → 재시도 폭풍"의 악순환을 낳습니다.

- **Redis Sorted Set**으로 대기열 구성 — `ZADD`(진입) / `ZRANK`(순번, O(log N)) / `ZPOPMIN`(배치 추출) / `ZCARD`(전체 인원). Set 특성으로 중복 진입도 자동 방지.
- 가벼운 진입 API(`ZADD` ~1ms)로 트래픽을 앞단에서 흡수하고, 무거운 주문 API(~200ms)에는 **토큰을 받은 소수만** 도달하게 함.
- **스케줄러**가 100ms마다 DB 처리 능력만큼만 토큰을 발급 → back-pressure 구현.
- **1회용 토큰**은 Redis Lua 스크립트로 `GET+DEL`을 원자화하여 동시 요청 중 하나만 성공하도록 보장.
- 대기열 ON/OFF는 **Redis Pub/Sub 피처 플래그**로 제어 — 멀티 인스턴스 환경에서 `volatile boolean` 로컬 캐시를 즉시 동기화(매 요청 Redis GET 회피).

#### 대기열 TPS 산정 근거

유입 제어의 목표 처리량은 감이 아니라 **인프라 실제 수용량에서 역산**했습니다.

| 항목 | 값 | 산출 근거 |
|------|-----|-----------|
| DB 커넥션 풀 | 40 | 프로젝트 실제 설정값 (`maximum-pool-size: 40`) |
| 주문 1건 평균 처리 시간 | 200ms | 과제 가정값 |
| 이론적 최대 TPS | 200 | 40 커넥션 ÷ 0.2초 |
| 안전 마진 | 70% | DB 외 연산 · GC · 네트워크 오버헤드 고려 |
| **목표 TPS** | **140** | 200 × 0.7 |
| 스케줄러 주기 | 100ms | 1초를 10분할 (순간 피크 완화) |
| **배치 크기** | **14** | 140 ÷ 10 |
| 토큰 TTL | 5분 | 유저 행동 시간(1~2분) + 안전 마진 |

> 한 번에 140개를 발급하면 순간적으로 커넥션을 100% 점유해 다른 API에 영향을 주므로, 100ms 간격으로 14개씩 나눠 발급하여 Thundering Herd를 완화했습니다.

### 2. 외부 PG 연동 회복력 — Circuit Breaker (Round 6)

PG 장애가 내부 시스템 전체로 번지지 않도록 Resilience4j로 방어했습니다.

- **Circuit Breaker**: 실패율 50% 초과 시 Open, 최근 10건 슬라이딩 윈도우, Open 후 10초 뒤 Half-Open. `slow-call-duration-threshold: 3s`로 **느린 응답도 실패로 간주**(지연 장애 차단).
- **Fallback — "즉시 실패"가 아닌 "PENDING 보류"**: PG timeout은 "실패"가 아니라 "모름"이기 때문. 실제로는 결제됐는데 응답만 유실됐을 수 있어, 결제 건을 PENDING으로 남기고 콜백/수동 동기화로 최종 확정.
- **Retry는 멱등 연산에만**: `requestPayment`는 이중 청구 위험 때문에 retry 제거, 조회 연산(`getTransaction`)에만 retry 적용. `retry-exceptions`도 `FeignException`(네트워크)만 남겨 서킷 오픈의 의미가 훼손되지 않게 함.
- 결제 실패 확정 시 **보상 트랜잭션**(`OrderCompensationService`)으로 쿠폰·재고 원복.

### 3. 캐싱 전략 (Round 5)

- 20만 건 기준 상품 조회에 **복합 인덱스** `(deleted_at, visibility, brand_id)` 적용 — 필수 필터를 선두에 두어 브랜드 필터 유무 모두 커버. 스캔 `rows` 199,176 → 18,810.
- **Redis 캐시**(멀티 인스턴스 캐시 일관성을 위해 로컬 캐시 대신 선택). 상품 상세 30분 / 목록 10분 TTL.
- 좋아요처럼 빈번한 변경은 evict 대신 **TTL 기반 Eventual Consistency**로 캐시 히트율 유지.
- 응답시간: 캐시 미스 770ms → 히트 10ms.

> **캐시 스탬피드(Cache Stampede)**: TTL 만료 직후 요청 쏠림 가능성을 리스크로 명시하고, 현재 규모에서는 허용하되 확대 시 Mutex Lock / Probabilistic Early Expiration 도입을 향후 과제로 정리해 두었습니다. (현시점 미구현)

### 4. 이벤트 기반 아키텍처 & 선착순 쿠폰 (Round 7)

- **Transactional Outbox Pattern**: 비즈니스 로직과 이벤트 발행의 원자성 보장. `BEFORE_COMMIT` Outbox INSERT → `AFTER_COMMIT` Kafka 발행 → 실패 시 스케줄러 polling 재시도로 **At Least Once** 보장.
- **Consumer 멱등성**: `event_handled` 테이블로 중복 소비 방지, `productId`를 partition key로 순서 보장. 카운터성 이벤트는 증감(+1/-1) 방식으로 집계.
- **선착순 쿠폰**: Redis atomic `INCR`로 즉시 컷오프(초과분은 불필요한 Kafka 메시지 차단) → 통과분만 Kafka로 비동기 발급하여 DB 쓰기 부하 평탄화. (200명 동시 요청 → 정확히 100건만 발급되는 동시성 E2E 검증)

### 5. 실시간 랭킹 & 배치 집계 (Round 9–10)

- **일간 랭킹**: Kafka 이벤트 → Redis ZSET `ZINCRBY`로 가중치 스코어(view 0.1 / like 0.2 / order 0.7 × 결제금액)를 실시간 갱신. 일별 키 + TTL로 시간 윈도우 관리.
- **주간/월간 랭킹**: Spring Batch **Chunk-Oriented Processing**으로 일별 집계 테이블을 롤링 윈도우(7일/30일)로 집계 → 스냅샷 테이블 적재. `beforeStep` DELETE + UNIQUE 제약으로 **재실행 멱등성** 보장, `ROW_NUMBER()`로 정확히 TOP 100 보장.

---

## 테스트 전략

- **단위 테스트**: 도메인 규칙 + InMemory/Fake Repository로 인프라 격리
- **통합 테스트**: TestContainers(MySQL/Redis), EmbeddedKafka, WireMock(PG 경계)
- **E2E 테스트**: `@SpringBootTest(RANDOM_PORT)` + 실제 API 호출. 동시성 시나리오는 `CountDownLatch` + `ExecutorService`로 검증 (선착순 쿠폰 200→100, 대기열 동시 진입 등)

TDD(Red → Green → Refactor)와 3A(Arrange-Act-Assert) 원칙 기반으로 작업했습니다.

---

## 실행 방법

```shell
# 인프라 (MySQL, Redis, Kafka)
docker-compose -f ./docker/infra-compose.yml up -d

# API 서버 실행
./gradlew :apps:commerce-api:bootRun --args='--spring.profiles.active=local'

# 테스트
./gradlew test
```
