---
slug: /sql-reference/table-functions/numbers
sidebar_position: 145
sidebar_label: 'numbers'
title: 'numbers'
description: '정수 시퀀스를 포함하는 단일 `number` 컬럼이 있는 테이블을 반환합니다.'
doc_type: 'reference'
---

* `numbers()` – 0부터 시작하는 정수를 오름차순으로 포함하는 단일 `number` 컬럼(UInt64)이 있는 무한한 테이블을 반환합니다. 행 수를 제한하려면 `LIMIT`(필요에 따라 `OFFSET`도 함께)를 사용하십시오.

* `numbers(N)` – 0부터 `N - 1`까지의 정수를 포함하는 단일 `number` 컬럼(UInt64)이 있는 테이블을 반환합니다.

* `numbers(N, M)` – `N`부터 `N + M - 1`까지의 정수 `M`개를 포함하는 단일 `number` 컬럼(UInt64)이 있는 테이블을 반환합니다.

* `numbers(N, M, S)` – `[N, N + M)` 범위의 값을 간격 `S`로 포함하는 단일 `number` 컬럼(UInt64)이 있는 테이블을 반환합니다(약 `M / S`개 행, 올림). `S`는 `>= 1`이어야 합니다.

이는 [`system.numbers`](/ko/operations/system-tables/numbers) 시스템 테이블과 유사합니다. 테스트 및 연속적인 값 생성에 사용할 수 있습니다.

다음 쿼리는 서로 동일합니다:

```sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM numbers() LIMIT 10;
SELECT * FROM system.numbers LIMIT 10;
SELECT * FROM system.numbers WHERE number BETWEEN 0 AND 9;
SELECT * FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
```

다음 쿼리도 동일하게 동작합니다:

```sql
SELECT * FROM numbers(10, 10);
SELECT * FROM numbers() LIMIT 10 OFFSET 10;
SELECT * FROM system.numbers LIMIT 10 OFFSET 10;
```

다음 쿼리도 동일한 의미입니다:

```sql
SELECT number * 2 FROM numbers(10);
SELECT (number - 10) * 2 FROM numbers(10, 10);
SELECT * FROM numbers(0, 20, 2);
```

<div id="examples">
  ### 예시
</div>

처음 10개의 수입니다.

```sql
SELECT * FROM numbers(10);
```

```response
 ┌─number─┐
 │      0 │
 │      1 │
 │      2 │
 │      3 │
 │      4 │
 │      5 │
 │      6 │
 │      7 │
 │      8 │
 │      9 │
 └────────┘
```

2010-01-01부터 2010-12-31까지 날짜 시퀀스를 생성합니다.

```sql
SELECT toDate('2010-01-01') + number AS d FROM numbers(365);
```

`sipHash64(number)`에 후행 0비트가 20개 있는 첫 번째 `UInt64` `>= 10^15`를 찾으십시오.

```sql
SELECT number
FROM numbers()
WHERE number >= 1e15
  AND bitAnd(sipHash64(number), 0xFFFFF) = 0
LIMIT 1;
```

```response
 ┌───────────number─┐
 │ 1000000000056095 │ -- 1.00 quadrillion
 └──────────────────┘
```

<div id="notes">
  ### 참고 사항
</div>

* 성능상 필요한 행 수를 알고 있다면 무제한 `numbers()` / `system.numbers`보다 범위가 지정된 형식(`numbers(N)`, `numbers(N, M[, S])`)을 사용하는 것이 좋습니다.
* 병렬 생성에는 `numbers_mt(...)` 또는 [`system.numbers_mt`](/ko/operations/system-tables/numbers_mt) 테이블을 사용하십시오. 결과는 임의의 순서로 반환될 수 있습니다.