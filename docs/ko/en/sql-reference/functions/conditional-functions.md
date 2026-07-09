---
description: '조건부 함수 문서'
sidebar_label: '조건부'
slug: /sql-reference/functions/conditional-functions
title: '조건부 함수'
doc_type: 'reference'
---

<div id="overview">
  ## 개요
</div>

<div id="using-conditional-results-directly">
  ### 조건식 결과 직접 사용하기
</div>

조건식의 결과는 항상 `0`, `1` 또는 `NULL`입니다. 따라서 다음과 같이 조건식의 결과를 직접 사용할 수 있습니다.

```sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

<div id="null-values-in-conditionals">
  ### 조건식에서의 NULL 값
</div>

조건식에 `NULL` 값이 포함되면 결과 역시 `NULL`이 됩니다.

```sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

따라서 타입이 `Nullable`인 경우 쿼리를 신중하게 작성해야 합니다.

다음 예시는 `multiIf`에 `equals` 조건을 추가하지 않으면 실패하는 상황을 보여줍니다.

```sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

<div id="case-statement">
  ### CASE 문
</div>

ClickHouse의 CASE 표현식은 SQL CASE 연산자와 유사한 조건부 로직을 제공합니다. 조건을 평가한 뒤, 가장 먼저 일치하는 조건에 따라 값을 반환합니다.

ClickHouse는 CASE를 두 가지 형식으로 지원합니다.

1. `CASE WHEN ... THEN ... ELSE ... END`
   <br />
   이 형식은 매우 유연하며, 내부적으로는 [multiIf](/ko/sql-reference/functions/conditional-functions#multiIf) 함수를 사용해 구현됩니다. 각 조건은 서로 독립적으로 평가되며, 표현식에는 상수가 아닌 값도 포함할 수 있습니다.

```sql
SELECT
    number,
    CASE
        WHEN number % 2 = 0 THEN number + 1
        WHEN number % 2 = 1 THEN number * 10
        ELSE number
    END AS result
FROM system.numbers
WHERE number < 5;

-- is translated to
SELECT
    number,
    multiIf((number % 2) = 0, number + 1, (number % 2) = 1, number * 10, number) AS result
FROM system.numbers
WHERE number < 5

┌─number─┬─result─┐
│      0 │      1 │
│      1 │     10 │
│      2 │      3 │
│      3 │     30 │
│      4 │      5 │
└────────┴────────┘

5 rows in set. Elapsed: 0.002 sec.
```

2. `CASE <expr> WHEN <val1> THEN ... WHEN <val2> THEN ... ELSE ... END`
   <br />
   이 더 간결한 형식은 상수 값과의 일치 여부를 확인하는 데 최적화되어 있으며, 내부적으로 `caseWithExpression()`을 사용합니다.

예시로, 다음 구문은 유효합니다:

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN 100
        WHEN 1 THEN 200
        ELSE 0
    END AS result
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, 100, 1, 200, 0) AS result
FROM system.numbers
WHERE number < 3

┌─number─┬─result─┐
│      0 │    100 │
│      1 │    200 │
│      2 │      0 │
└────────┴────────┘

3 rows in set. Elapsed: 0.002 sec.
```

이 구문에서도 반환 표현식이 상수일 필요는 없습니다.

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN number + 1
        WHEN 1 THEN number * 10
        ELSE number
    END
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, number + 1, 1, number * 10, number)
FROM system.numbers
WHERE number < 3

┌─number─┬─caseWithExpr⋯0), number)─┐
│      0 │                        1 │
│      1 │                       10 │
│      2 │                        2 │
└────────┴──────────────────────────┘

3 rows in set. Elapsed: 0.001 sec.
```

<div id="caveats">
  #### 주의 사항
</div>

ClickHouse는 조건을 평가하기 전에 CASE 표현식(또는 `multiIf`와 같은 내부적으로 동등한 표현식)의 결과 타입을 결정합니다. 이는 반환 표현식의 타입이 서로 다를 때, 예를 들어 시간대나 숫자 타입이 서로 다른 경우 특히 중요합니다.

* 결과 타입은 모든 분기 중에서 가장 큰 호환 가능 타입을 기준으로 선택됩니다.
* 이 타입이 선택되고 나면, 다른 모든 분기는 해당 타입으로 암묵적으로 CAST됩니다. 런타임에 해당 로직이 실제로 실행되지 않더라도 마찬가지입니다.
* DateTime64처럼 시간대가 타입 시그니처의 일부인 타입에서는 이로 인해 예상과 다른 동작이 발생할 수 있습니다. 즉, 다른 분기에서 서로 다른 시간대를 지정하더라도 처음 나타난 시간대가 모든 분기에 사용될 수 있습니다.

예를 들어, 아래에서는 모든 행이 처음으로 일치한 분기의 시간대, 즉 `Asia/Kolkata` 기준의 timestamp를 반환합니다.

```sql
SELECT
    number,
    CASE
        WHEN number = 0 THEN fromUnixTimestamp64Milli(0, 'Asia/Kolkata')
        WHEN number = 1 THEN fromUnixTimestamp64Milli(0, 'America/Los_Angeles')
        ELSE fromUnixTimestamp64Milli(0, 'UTC')
    END AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, fromUnixTimestamp64Milli(0, 'Asia/Kolkata'), number = 1, fromUnixTimestamp64Milli(0, 'America/Los_Angeles'), fromUnixTimestamp64Milli(0, 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬──────────────────────tz─┐
│      0 │ 1970-01-01 05:30:00.000 │
│      1 │ 1970-01-01 05:30:00.000 │
│      2 │ 1970-01-01 05:30:00.000 │
└────────┴─────────────────────────┘

3 rows in set. Elapsed: 0.011 sec.
```

여기서 ClickHouse는 여러 `DateTime64(3, <timezone>)` 반환 타입을 확인합니다. ClickHouse는 가장 먼저 확인한 `DateTime64(3, 'Asia/Kolkata'`를 공통 타입으로 추론하고, 다른 분기는 암묵적으로 이 타입으로 캐스팅합니다.

이 문제는 의도한 시간대 포맷을 유지할 수 있도록 문자열로 변환하여 해결할 수 있습니다:

```sql
SELECT
    number,
    multiIf(
        number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'),
        number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'),
        formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')
    ) AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'), number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'), formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬─tz──────────────────┐
│      0 │ 1970-01-01 05:30:00 │
│      1 │ 1969-12-31 16:00:00 │
│      2 │ 1970-01-01 00:00:00 │
└────────┴─────────────────────┘

3 rows in set. Elapsed: 0.002 sec.
```

{/* 
  아래 태그 내부의 콘텐츠는 문서 프레임워크 build 시점에
  system.functions에서 생성된 문서로 대체됩니다. 태그를 수정하거나 제거하지 마십시오.
  참고: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }