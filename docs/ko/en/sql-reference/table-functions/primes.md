---
slug: /sql-reference/table-functions/primes
sidebar_position: 145
sidebar_label: 'primes'
title: 'primes'
description: '소수를 포함하는 단일 `prime` 컬럼이 있는 테이블을 반환합니다.'
doc_type: 'reference'
---

* `primes()` – 2부터 시작하는 소수를 오름차순으로 담은 단일 `prime` 컬럼(UInt64)으로 이루어진 무한 테이블을 반환합니다. 행 수를 제한하려면 `LIMIT`(필요한 경우 `OFFSET`)를 사용하십시오.

* `primes(N)` – 2부터 시작하는 처음 `N`개의 소수를 담은 단일 `prime` 컬럼(UInt64)으로 이루어진 테이블을 반환합니다.

* `primes(N, M)` – `N`번째 소수(0부터 시작)부터 시작하는 `M`개의 소수를 담은 단일 `prime` 컬럼(UInt64)으로 이루어진 테이블을 반환합니다.

* `primes(N, M, S)` – 소수 순번 기준으로 간격 `S`를 두고 `N`번째 소수(0부터 시작)부터 시작하는 `M`개의 소수를 담은 단일 `prime` 컬럼(UInt64)으로 이루어진 테이블을 반환합니다. 반환되는 소수는 인덱스 `N, N + S, N + 2S, ..., N + (M - 1)S`에 해당합니다. `S`는 `>= 1`이어야 합니다.

이는 [`system.primes`](/ko/operations/system-tables/primes) 시스템 테이블과 유사합니다.

다음 쿼리는 동일합니다:

```sql
SELECT * FROM primes(10);
SELECT * FROM primes(0, 10);
SELECT * FROM primes() LIMIT 10;
SELECT * FROM system.primes LIMIT 10;
SELECT * FROM system.primes WHERE prime IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
```

다음 쿼리도 같은 의미입니다:

```sql
SELECT * FROM primes(10, 10);
SELECT * FROM primes() LIMIT 10 OFFSET 10;
SELECT * FROM system.primes LIMIT 10 OFFSET 10;
```

<div id="examples">
  ### 예시
</div>

처음 10개의 소수입니다.

```sql
SELECT * FROM primes(10);
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

1e15를 초과하는 첫 번째 소수.

```sql
SELECT prime FROM primes() WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```

매우 큰 범위에서 소수에 대한 modulo 제약을 풉니다: `p` modulo `65537`의 값이 `1`이 되는 `p >= 10^15`인 첫 번째 소수 `p`를 찾습니다.

```sql
SELECT prime
FROM primes()
WHERE prime >= 1e15
  AND prime % 65537 = 1
LIMIT 1;
```

```response
 ┌────────────prime─┐
 │ 1000000001218399 │ -- 1.00 quadrillion
 └──────────────────┘
```

첫 7개의 메르센 소수.

```sql
SELECT prime
FROM primes()
WHERE bitAnd(prime, prime + 1) = 0
LIMIT 7;
```

```response
  ┌──prime─┐
  │      3 │
  │      7 │
  │     31 │
  │    127 │
  │   8191 │
  │ 131071 │
  │ 524287 │
  └────────┘
```

<div id="notes">
  ### 참고 사항
</div>

* 가장 빠른 형태는 기본 간격(`1`)을 사용하는 단순 범위 쿼리와 점 필터 쿼리입니다. 예를 들어 `primes(N)` 또는 `primes() LIMIT N`이 있습니다. 이러한 형태는 최적화된 소수 생성기를 사용해 매우 큰 소수도 효율적으로 계산합니다.
* 경계가 없는 소스(`primes()` / `system.primes`)에는 `prime BETWEEN ...`, `prime IN (...)`, `prime = ...`와 같은 단순한 값 필터를 생성 과정에서 적용해 탐색할 값 범위를 제한할 수 있습니다. 예를 들어, 다음 쿼리는 거의 즉시 실행됩니다:

```sql
SELECT sum(prime)
FROM primes()
WHERE prime BETWEEN 1e6 AND 1e6 + 100
   OR prime BETWEEN 1e12 AND 1e12 + 100
   OR prime BETWEEN 1e15 AND 1e15 + 100
   OR prime IN (9999999967, 9999999971, 9999999973)
   OR prime = 1000000000000037;
```

```response
  ┌───────sum(prime)─┐
  │ 2004010006000641 │ -- 2.00 quadrillion
  └──────────────────┘

1 row in set. Elapsed: 0.090 sec. 
```

* 이 값 범위 최적화는 `WHERE`가 있는 범위가 제한된 테이블 함수(`primes(N)`, `primes(offset, count[, step])`)에는 적용되지 않습니다. 이러한 변형은 소수 인덱스를 기준으로 유한한 테이블을 정의하므로, 의미를 유지하려면 해당 테이블을 생성한 뒤에 필터를 평가해야 합니다.
* 0이 아닌 offset 및/또는 1보다 큰 간격(`primes(offset, count)` / `primes(offset, count, step)`)을 사용하면 내부적으로 추가 소수를 생성하고 건너뛰어야 할 수 있으므로 더 느릴 수 있습니다. offset이나 간격이 필요하지 않다면 생략하십시오.