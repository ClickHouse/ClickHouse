---
description: '집계 함수에 대한 문서'
sidebar_label: '집계 함수'
sidebar_position: 33
slug: /sql-reference/aggregate-functions/
title: '집계 함수'
doc_type: 'reference'
---

집계 함수는 데이터베이스 전문가가 예상하는 [통상적인](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) 방식으로 동작합니다.

ClickHouse는 다음 기능도 지원합니다.

* [매개변수화된 집계 함수](/ko/sql-reference/aggregate-functions/parametric-functions): 컬럼 외에 다른 매개변수도 받습니다.
* [Combinators](/ko/sql-reference/aggregate-functions/combinators): 집계 함수의 동작 방식을 변경합니다.

<div id="null-processing">
  ## NULL 처리
</div>

집계 시 모든 `NULL` 인수는 건너뜁니다. 집계에 인수가 여러 개 있는 경우, 그중 하나 이상이 NULL인 행은 모두 무시됩니다.

이 규칙에는 예외가 있습니다. [`first_value`](../../sql-reference/aggregate-functions/reference/first_value.md), [`last_value`](../../sql-reference/aggregate-functions/reference/last_value.md) 함수와 해당 별칭(`any`, `anyLast`)은 수정자 `RESPECT NULLS`가 뒤에 오는 경우 예외입니다. 예를 들어 `FIRST_VALUE(b) RESPECT NULLS`가 있습니다.

**예시:**

다음 테이블을 살펴보겠습니다:

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

`y` 컬럼의 값 합계를 구해야 한다고 가정해 보겠습니다:

```sql
SELECT sum(y) FROM t_null_big
```

```text
┌─sum(y)─┐
│      7 │
└────────┘
```

이제 `groupArray` 함수를 사용해 `y` 컬럼으로 배열을 생성할 수 있습니다:

```sql
SELECT groupArray(y) FROM t_null_big
```

```text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray`는 결과 배열에 `NULL`을 포함하지 않습니다.

[COALESCE](../../sql-reference/functions/functions-for-nulls.md#coalesce)를 사용하면 `NULL`을 사용 사례에 맞는 값으로 바꿀 수 있습니다. 예를 들어 `avg(COALESCE(column, 0))`는 집계 시 컬럼 값을 사용하고, `NULL`이면 0을 사용합니다:

```sql
SELECT
    avg(y),
    avg(coalesce(y, 0))
FROM t_null_big
```

```text
┌─────────────avg(y)─┬─avg(coalesce(y, 0))─┐
│ 2.3333333333333335 │                 1.4 │
└────────────────────┴─────────────────────┘
```

또한 [Tuple](/ko/sql-reference/data-types/tuple.md)을 사용해 NULL 스키핑 동작을 우회할 수도 있습니다. `NULL` 값만 들어 있는 `Tuple`은 `NULL`이 아니므로, 집계 함수는 그 `NULL` 값 때문에 해당 행을 건너뛰지 않습니다.

```sql
SELECT
    groupArray(y),
    groupArray(tuple(y)).1
FROM t_null_big;

┌─groupArray(y)─┬─tupleElement(groupArray(tuple(y)), 1)─┐
│ [2,2,3]       │ [2,NULL,2,3,NULL]                     │
└───────────────┴───────────────────────────────────────┘
```

컬럼이 집계 함수의 인수로 사용되면 집계가 수행되지 않는다는 점에 유의하십시오.  예를 들어 매개변수가 없는 [`count`](../../sql-reference/aggregate-functions/reference/count.md) (`count()`) 또는 상수 매개변수를 사용하는 `count(1)`은 블록의 모든 행을 계산합니다(`GROUP BY` 컬럼은 인수가 아니므로 그 값과 무관함). 반면 `count(column)`은 `column`이 NULL이 아닌 행의 수만 반환합니다.

```sql
SELECT
    v,
    count(1),
    count(v)
FROM
(
    SELECT if(number < 10, NULL, number % 3) AS v
    FROM numbers(15)
)
GROUP BY v

┌────v─┬─count()─┬─count(v)─┐
│ ᴺᵁᴸᴸ │      10 │        0 │
│    0 │       1 │        1 │
│    1 │       2 │        2 │
│    2 │       2 │        2 │
└──────┴─────────┴──────────┘
```

다음은 `RESPECT NULLS`를 사용하는 first&#95;value의 예시이며, 여기서 NULL 입력도 그대로 반영되어 NULL 여부와 관계없이 처음 읽은 값을 반환한다는 것을 확인할 수 있습니다:

```sql
SELECT
    col || '_' || ((col + 1) * 5 - 1) AS range,
    first_value(odd_or_null) AS first,
    first_value(odd_or_null) IGNORE NULLS as first_ignore_null,
    first_value(odd_or_null) RESPECT NULLS as first_respect_nulls
FROM
(
    SELECT
        intDiv(number, 5) AS col,
        if(number % 2 == 0, NULL, number) AS odd_or_null
    FROM numbers(15)
)
GROUP BY col
ORDER BY col

┌─range─┬─first─┬─first_ignore_null─┬─first_respect_nulls─┐
│ 0_4   │     1 │                 1 │                ᴺᵁᴸᴸ │
│ 1_9   │     5 │                 5 │                   5 │
│ 2_14  │    11 │                11 │                ᴺᵁᴸᴸ │
└───────┴───────┴───────────────────┴─────────────────────┘
```