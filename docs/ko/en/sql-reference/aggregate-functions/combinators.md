---
description: '집계 함수 조합자에 대한 문서'
sidebar_label: '조합자'
sidebar_position: 37
slug: /sql-reference/aggregate-functions/combinators
title: '집계 함수 조합자'
doc_type: 'reference'
---

집계 함수 이름 뒤에 접미사를 붙일 수 있습니다. 그러면 집계 함수의 동작 방식이 달라집니다.

<div id="-if">
  ## -If
</div>

접미사 -If는 모든 집계 함수 이름에 추가할 수 있습니다. 이 경우 집계 함수는 추가 인수인 조건(`Uint8` 타입)을 받습니다. 집계 함수는 조건을 만족하는 행만 처리합니다. 조건이 한 번도 충족되지 않으면 기본값(일반적으로 0 또는 빈 문자열)을 반환합니다.

예시: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` 등입니다.

조건부 집계 함수를 사용하면 서브쿼리와 `JOIN`을 사용하지 않고도 여러 조건에 대한 집계 결과를 한 번에 계산할 수 있습니다. 예를 들어, 조건부 집계 함수는 세그먼트 비교 기능을 구현하는 데 사용할 수 있습니다.

<div id="-array">
  ## -Array
</div>

-Array 접미사는 모든 집계 함수에 추가할 수 있습니다. 이 경우 집계 함수는 &#39;T&#39; 유형의 인수 대신 &#39;Array(T)&#39; 유형(배열)의 인수를 받습니다. 집계 함수가 여러 인수를 받는다면, 그 인수들은 길이가 같은 배열이어야 합니다. 배열을 처리할 때 집계 함수는 모든 배열 요소에 대해 원래 집계 함수와 같은 방식으로 동작합니다.

예시 1: `sumArray(arr)` - 모든 &#39;arr&#39; 배열의 모든 요소를 합산합니다. 이 예시에서는 `sum(arraySum(arr))`로 더 간단하게 작성할 수도 있습니다.

예시 2: `uniqArray(arr)` – 모든 &#39;arr&#39; 배열에서 고유 요소의 개수를 계산합니다. 이는 `uniq(arrayJoin(arr))`로 더 쉽게 수행할 수 있지만, 쿼리에 항상 &#39;arrayJoin&#39;을 추가할 수 있는 것은 아닙니다.

-If와 -Array는 함께 사용할 수 있습니다. 하지만 &#39;Array&#39;가 먼저 오고 그다음에 &#39;If&#39;가 와야 합니다. 예시: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. 이 순서 때문에 &#39;cond&#39; 인수는 배열이 아닙니다.

<div id="-map">
  ## -Map
</div>

-Map 접미사는 모든 집계 함수에 추가할 수 있습니다. 이렇게 하면 맵(Map) 타입을 인수로 받아, 지정된 집계 함수를 사용해 맵의 각 키에 해당하는 값을 각각 집계하는 집계 함수가 생성됩니다. 결과 역시 맵(Map) 타입입니다.

**예시**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = MergeTree
ORDER BY ();

INSERT INTO map_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', (['a', 'b', 'c'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', (['c', 'd', 'e'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['d', 'e', 'f'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['f', 'g', 'g'], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(status),
    avgMap(status),
    minMap(status)
FROM map_map
GROUP BY timeslot;

┌────────────timeslot─┬─sumMap(status)───────────────────────┬─avgMap(status)───────────────────────┬─minMap(status)───────────────────────┐
│ 2000-01-01 00:00:00 │ {'a':10,'b':10,'c':20,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │
│ 2000-01-01 00:01:00 │ {'d':10,'e':10,'f':20,'g':20}        │ {'d':10,'e':10,'f':10,'g':10}        │ {'d':10,'e':10,'f':10,'g':10}        │
└─────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┘
```

<div id="-simplestate">
  ## -SimpleState
</div>

이 조합자를 적용하면 집계 함수는 동일한 값을 반환하지만 타입은 달라집니다. 이는 테이블에 저장하여 [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 테이블에서 사용할 수 있는 [SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md)입니다.

**구문**

```sql
<aggFunction>SimpleState(x)
```

**인수**

* `x` — 집계 함수의 매개변수입니다.

**반환 값**

`SimpleAggregateFunction(...)` 유형의 집계 함수 값입니다.

**예시**

```sql title="Query"
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

```text title="Response"
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

<div id="-state">
  ## -State
</div>

이 조합자를 적용하면 집계 함수는 결과값(예: [uniq](/ko/sql-reference/aggregate-functions/reference/uniq) 함수의 고유값 개수)을 반환하지 않고 집계의 중간 상태를 반환합니다(`uniq`의 경우 고유값 개수를 계산하기 위한 해시 테이블입니다). 이는 이후 추가 처리에 사용하거나 나중에 집계를 완료할 수 있도록 테이블에 저장할 수 있는 `AggregateFunction(...)`입니다.

:::note
중간 상태의 데이터 순서는 달라질 수 있으므로, 동일한 데이터에 대해서도 -MapState는 불변이 아니라는 점에 유의하십시오. 다만 이는 이 데이터의 수집에는 영향을 주지 않습니다.
:::

이러한 상태를 다루려면 다음을 사용하십시오:

* [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 테이블 엔진.
* [finalizeAggregation](/ko/sql-reference/functions/other-functions#finalizeAggregation) 함수.
* [runningAccumulate](../../sql-reference/functions/other-functions.md#runningAccumulate) 함수.
* [-Merge](#-merge) 조합자.
* [-MergeState](#-mergestate) 조합자.

<div id="-merge">
  ## -Merge
</div>

이 조합자를 적용하면 집계 함수는 중간 집계 상태를 인수로 받아 이 상태들을 결합해 집계를 완료한 뒤, 결과 값을 반환합니다.

<div id="-mergestate">
  ## -MergeState
</div>

`-Merge` 결합자(조합자)와 동일한 방식으로 중간 집계 상태(aggregation states)를 머지합니다. 다만 결과값을 반환하는 대신, `-State` 결합자와 마찬가지로 중간 집계 상태를 반환합니다.

<div id="-foreach">
  ## -ForEach
</div>

테이블(table)용 집계 함수를, 각 배열에서 같은 위치의 항목들을 집계하고 결과 배열을 반환하는 배열용 집계 함수로 변환합니다. 예를 들어, 배열 `[1, 2]`, `[3, 4, 5]` 및 `[6, 7]`에 대한 `sumForEach`는 같은 위치의 배열 항목들을 더해 `[10, 13, 5]`를 반환합니다.

<div id="-tuple">
  ## -Tuple
</div>

`-Tuple` 접미사는 모든 집계 함수에 추가할 수 있습니다. 이렇게 결합된 함수는 기반 집계 함수의 각 인수에 대응하는 `Tuple` 유형의 인수를 하나씩 받으며, 모든 튜플은 동일한 개수의 요소를 가져야 합니다. 집계는 각 요소 위치에 대해 독립적으로 적용되며, 모든 `Tuple`에서 해당 위치의 요소를 받아 결과로 `Tuple`을 반환합니다.

첫 번째 입력 `Tuple`에 명시적인 요소 이름이 있으면 그 이름은 결과에도 유지됩니다.

`NULL` 값을 자체적으로 처리하는 집계 함수(`anyRespectNulls`, `anyLastRespectNulls`, `RESPECT NULLS` 수정자)는 `Nullable(Tuple(...))` 유형을 인수로 지원하지 않습니다. 대신 `Nullable` 요소를 사용하십시오.

**구문**

```sql
<aggFunction>Tuple(tuple1[, tuple2, ...])
```

**인수**

* `tuple1[, tuple2, ...]` — `Tuple` 유형의 컬럼이며, 기반 집계 함수의 각 인수에 대해 하나씩 대응하고 모두 동일한 개수의 요소를 가집니다. 각 요소는 해당 인수 위치에서 기반 집계 함수가 지원하는 유형이어야 합니다.

**반환 값**

* 각 요소에 집계 함수를 개별적으로 적용한 결과를 담은 `Tuple`.

유형: `Tuple(aggFunction(element1), aggFunction(element2), ...)`.

**예시**

쿼리:

```sql
SELECT sumTuple(t) FROM
(
    SELECT tuple(toInt64(1), toFloat64(2.5)) AS t
    UNION ALL
    SELECT tuple(toInt64(3), toFloat64(4.5))
    UNION ALL
    SELECT tuple(toInt64(5), toFloat64(6.5))
);
```

결과:

```text
┌─sumTuple(t)─┐
│ (9,13.5)    │
└─────────────┘
```

`GROUP BY`와 함께 사용하기:

```sql
SELECT
    k,
    avgTuple(t)
FROM
(
    SELECT
        number % 2 AS k,
        tuple(toInt64(number), toFloat64(number) * 1.5) AS t
    FROM numbers(6)
)
GROUP BY k
ORDER BY k;
```

```text
┌─k─┬─avgTuple(t)─┐
│ 0 │ (2,3)       │
│ 1 │ (3,4.5)     │
└───┴─────────────┘
```

다중 인수 집계 함수와 함께 사용하는 경우, 각 `Tuple` 인수는 기반 함수의 인수 하나를 제공하며 요소는 위치별로 서로 대응됩니다:

```text
corrTuple((a1, a2), (b1, b2)) = (corr(a1, b1), corr(a2, b2))
```

```sql
SELECT corrTuple((a1, a2), (b1, b2))
FROM
(
    SELECT
        toFloat64(number) AS a1,
        toFloat64(number * 2) AS a2,
        toFloat64(100 - number) AS b1,
        toFloat64(number * 3) AS b2
    FROM numbers(10)
);
```

```text
┌─corrTuple((a1, a2), (b1, b2))─┐
│ (-1,1)                        │
└───────────────────────────────┘
```

`a1`과 `b1`은 서로 반비례하고, `a2`와 `b2`는 비례하므로 결과는 `(-1, 1)`입니다.

`-Tuple`은 `-If`와 같은 다른 조합자와 조합해 사용할 수 있습니다. 예시: `sumTupleIf(tuple_column, cond)`.

<div id="-distinct">
  ## -Distinct
</div>

인수의 각 고유 조합은 한 번만 집계됩니다. 중복되는 값은 무시됩니다.
예시: `sum(DISTINCT x)` (또는 `sumDistinct(x)`), `groupArray(DISTINCT x)` (또는 `groupArrayDistinct(x)`), `corrStable(DISTINCT x, y)` (또는 `corrStableDistinct(x, y)`) 등입니다.

<div id="-ordefault">
  ## -OrDefault
</div>

집계 함수의 동작을 변경합니다.

집계 함수에 입력값이 없을 경우, 이 조합자를 사용하면 반환 데이터 타입의 기본값을 반환합니다. 입력 데이터가 비어 있을 수 있는 집계 함수에 적용됩니다.

`-OrDefault`는 다른 조합자와 함께 사용할 수 있습니다.

**구문**

```sql
<aggFunction>OrDefault(x)
```

**인수**

* `x` — 집계 함수의 매개변수입니다.

**반환 값**

집계할 값이 없으면 집계 함수 반환 유형의 기본값을 반환합니다.

유형은 사용하는 집계 함수에 따라 달라집니다.

**예시**

```sql title="Query"
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

```text title="Response"
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

또한 `-OrDefault`는 다른 조합자와 함께 사용할 수도 있습니다. 집계 함수가 빈 입력을 받지 못할 때 유용합니다.

```sql title="Query"
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

<div id="-ornull">
  ## -OrNull
</div>

집계 함수의 동작을 변경합니다.

이 조합자는 집계 함수의 결과를 [널 허용](../../sql-reference/data-types/nullable.md) 데이터 타입으로 변환합니다. 집계 함수가 계산할 값을 갖고 있지 않으면 [NULL](/ko/operations/settings/formats#input_format_null_as_default)을 반환합니다.

`-OrNull`은 다른 조합자와 함께 사용할 수 있습니다.

**구문**

```sql
<aggFunction>OrNull(x)
```

**인수**

* `x` — 집계 함수의 인수입니다.

**반환 값**

* 집계 함수의 결과를 `Nullable` 데이터 타입으로 변환한 값입니다.
* 집계할 값이 없으면 `NULL`입니다.

유형: `Nullable(aggregate function return type)`.

**예시**

집계 함수 이름 끝에 `-orNull`을 추가합니다.

```sql title="Query"
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

```text title="Response"
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

또한 `-OrNull`은 다른 조합자와도 함께 사용할 수 있습니다. 이는 집계 함수가 빈 입력을 받지 못할 때 유용합니다.

```sql title="Query"
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

<div id="-resample">
  ## -Resample
</div>

데이터를 그룹으로 나눈 뒤, 각 그룹의 데이터를 별도로 집계할 수 있습니다. 그룹은 하나의 컬럼 값을 인터벌로 나누어 생성됩니다.

```sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**인수**

* `start` — `resampling_key` 값에 대한 전체 인터벌의 시작 값입니다.
* `stop` — `resampling_key` 값에 대한 전체 인터벌의 종료 값입니다. 전체 인터벌에는 `stop` 값이 포함되지 않습니다 `[start, stop)`.
* `step` — 전체 인터벌을 하위 인터벌로 나누는 간격입니다. `aggFunction`은 각 하위 인터벌에서 독립적으로 실행됩니다.
* `resampling_key` — 값에 따라 데이터를 인터벌로 구분하는 데 사용하는 컬럼입니다.
* `aggFunction_params` — `aggFunction`의 매개변수입니다.

**반환 값**

* 각 하위 인터벌에 대한 `aggFunction` 결과의 배열입니다.

**예시**

다음과 같은 데이터가 있는 `people` 테이블(table)을 살펴보겠습니다:

```text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

연령이 `[30,60)` 및 `[60,75)` 인터벌에 속하는 사람들의 이름을 구해 보겠습니다. 연령은 정수로 표현하므로 실제로는 `[30, 59]` 및 `[60,74]` 인터벌의 연령을 얻게 됩니다.

이름을 배열로 집계하려면 [groupArray](/ko/sql-reference/aggregate-functions/reference/grouparray) 집계 함수를 사용합니다. 이 함수는 인수를 1개 받습니다. 여기서는 `name` 컬럼입니다. `groupArrayResample` 함수는 이름을 연령별로 집계하기 위해 `age` 컬럼을 사용해야 합니다. 필요한 인터벌을 정의하려면 `30, 75, 30` 인수를 `groupArrayResample` 함수에 전달합니다.

```sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

```text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

결과를 살펴보겠습니다.

`John`은 너무 어려서 샘플에 포함되지 않습니다. 다른 사람들은 지정된 연령 인터벌에 따라 분류됩니다.

이제 지정된 연령 인터벌별 전체 인원수와 평균 임금을 계산해 보겠습니다.

```sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

```text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

<div id="-argmin">
  ## -ArgMin
</div>

접미사 -ArgMin은 모든 집계 함수 이름에 추가할 수 있습니다. 이 경우 집계 함수는 추가 인수 하나를 받으며, 이 인수는 비교 가능한 표현식이면 됩니다. 집계 함수는 지정된 추가 표현식의 값이 최소인 행만 처리합니다.

예시: `sumArgMin(column, expr)`, `countArgMin(expr)`, `avgArgMin(x, expr)` 등.

<div id="-argmax">
  ## -ArgMax
</div>

접미사 -ArgMin과 비슷하지만, 지정된 추가 표현식의 최댓값을 가진 행만 처리합니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 배열, 맵, 상태에 집계 조합자 사용하기](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)