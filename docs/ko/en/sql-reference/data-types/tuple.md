---
description: 'ClickHouse의 튜플 데이터 타입 문서'
sidebar_label: '튜플(T1, T2, ...)'
sidebar_position: 34
slug: /sql-reference/data-types/tuple
title: '튜플(T1, T2, ...)'
doc_type: 'reference'
---

각 요소가 고유한 [타입](/ko/sql-reference/data-types)을 갖는 튜플입니다. 튜플은 최소 하나의 요소를 포함해야 합니다.

튜플은 컬럼을 임시로 그룹화할 때 사용됩니다. 쿼리에서 IN 표현식을 사용할 때 컬럼을 그룹화할 수 있으며, 람다 함수의 특정 매개변수를 지정할 때도 사용할 수 있습니다. 자세한 내용은 [IN operators](../../sql-reference/operators/in.md) 및 [Higher order functions](/ko/sql-reference/functions/overview#higher-order-functions) 섹션을 참조하십시오.

튜플은 쿼리 결과가 될 수도 있습니다. 이 경우 JSON 이외의 텍스트 형식에서는 값이 `()` 안에 쉼표로 구분되어 표시됩니다. JSON 포맷에서는 튜플이 배열(`[]`)로 출력됩니다.

<div id="creating-tuples">
  ## 튜플 생성
</div>

함수를 사용해 튜플을 만들 수 있습니다:

```sql
tuple(T1, T2, ...)
```

튜플 생성 예시:

```sql
SELECT tuple(1, 'a') AS x, toTypeName(x)
```

```text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

튜플은 요소를 하나만 가질 수도 있습니다

예시:

```sql
SELECT tuple('a') AS x;
```

```text
┌─x─────┐
│ ('a') │
└───────┘
```

`(tuple_element1, tuple_element2)` 구문을 사용하면 `tuple()` 함수를 호출하지 않고도 여러 요소로 구성된 튜플을 만들 수 있습니다.

예시:

```sql
SELECT (1, 'a') AS x, (today(), rand(), 'someString') AS y, ('a') AS not_a_tuple;
```

```text
┌─x───────┬─y──────────────────────────────────────┬─not_a_tuple─┐
│ (1,'a') │ ('2022-09-21',2006973416,'someString') │ a           │
└─────────┴────────────────────────────────────────┴─────────────┘
```

<div id="data-type-detection">
  ## 데이터 타입 감지
</div>

즉석에서 튜플을 생성할 때 ClickHouse는 제공된 인수 값을 저장할 수 있는 가장 작은 타입으로 튜플 인수의 타입을 추론합니다. 값이 [NULL](/ko/operations/settings/formats#input_format_null_as_default)이면 추론되는 타입은 [널 허용](../../sql-reference/data-types/nullable.md)입니다.

자동 데이터 타입 감지 예시:

```sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

```text
┌─x─────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1, NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└───────────┴─────────────────────────────────┘
```

<div id="referring-to-tuple-elements">
  ## 튜플 요소 참조
</div>

튜플 요소는 이름이나 인덱스로 참조할 수 있습니다:

```sql title="Query"
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;
INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples; -- by name
SELECT a.2 FROM named_tuples; -- by index
```

```text title="Response"
┌─a.s─┐
│ y   │
│ x   │
└─────┘

┌─tupleElement(a, 2)─┐
│                 10 │
│                -10 │
└────────────────────┘
```

<div id="comparison-operations-with-tuple">
  ## 튜플 비교 연산
</div>

두 튜플은 왼쪽에서 오른쪽 순서로 각 요소를 차례대로 비교합니다. 첫 번째 튜플의 요소가 두 번째 튜플의 대응하는 요소보다 크면(또는 작으면) 첫 번째 튜플이 두 번째 튜플보다 큰 것(또는 작은 것)으로 판단합니다. 그렇지 않고 두 요소가 같으면 다음 요소를 비교합니다.

예시:

```sql
SELECT (1, 'z') > (1, 'a') c1, (2022, 01, 02) > (2023, 04, 02) c2, (1,2,3) = (3,2,1) c3;
```

```text
┌─c1─┬─c2─┬─c3─┐
│  1 │  0 │  0 │
└────┴────┴────┘
```

실제 예시:

```sql
CREATE TABLE test
(
    `year` Int16,
    `month` Int8,
    `day` Int8
)
ENGINE = Memory AS
SELECT *
FROM values((2022, 12, 31), (2000, 1, 1));

SELECT * FROM test;

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
│ 2000 │     1 │   1 │
└──────┴───────┴─────┘

SELECT *
FROM test
WHERE (year, month, day) > (2010, 1, 1);

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
└──────┴───────┴─────┘
CREATE TABLE test
(
    `key` Int64,
    `duration` UInt32,
    `value` Float64
)
ENGINE = Memory AS
SELECT *
FROM values((1, 42, 66.5), (1, 42, 70), (2, 1, 10), (2, 2, 0));

SELECT * FROM test;

┌─key─┬─duration─┬─value─┐
│   1 │       42 │  66.5 │
│   1 │       42 │    70 │
│   2 │        1 │    10 │
│   2 │        2 │     0 │
└─────┴──────────┴───────┘

-- Let's find a value for each key with the biggest duration, if durations are equal, select the biggest value

SELECT
    key,
    max(duration),
    argMax(value, (duration, value))
FROM test
GROUP BY key
ORDER BY key ASC;

┌─key─┬─max(duration)─┬─argMax(value, tuple(duration, value))─┐
│   1 │            42 │                                    70 │
│   2 │             2 │                                     0 │
└─────┴───────────────┴───────────────────────────────────────┘
```

<div id="nullable-tuple">
  ## Nullable(튜플(T1, T2, ...))
</div>

:::note 베타 기능
`SET enable_nullable_tuple_type = 1`이 필요합니다
이 기능은 베타입니다.
:::

`Tuple(Nullable(T1), Nullable(T2), ...)`처럼 개별 요소만 `NULL`이 될 수 있는 것과 달리, 이 유형은 튜플 전체를 `NULL`로 허용합니다.

| 유형                                         | 튜플 전체가 NULL일 수 있음 | 요소가 NULL일 수 있음 |
| ------------------------------------------ | -------------------- | -------------- |
| `Nullable(Tuple(String, Int64))`           | ✅                    | ❌              |
| `Tuple(Nullable(String), Nullable(Int64))` | ❌                    | ✅              |

예시:

```sql
SET enable_nullable_tuple_type = 1;

CREATE TABLE test (
    id UInt32,
    data Nullable(Tuple(String, Int64))
) ENGINE = Memory;

INSERT INTO test VALUES (1, ('hello', 42)), (2, NULL);

SELECT * FROM test WHERE data IS NULL;
```

```txt
 ┌─id─┬─data─┐
 │  2 │ ᴺᵁᴸᴸ │
 └────┴──────┘
```