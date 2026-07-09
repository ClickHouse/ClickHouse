---
description: 'arrayJoin 함수 문서'
sidebar_label: 'arrayJoin'
slug: /sql-reference/functions/array-join
title: 'arrayJoin 함수'
doc_type: 'reference'
---

이 함수는 매우 독특한 함수입니다.

일반 함수는 행 집합을 변경하지 않고 각 행의 값만 바꿉니다(맵).
집계 함수는 행 집합을 압축합니다(폴드 또는 리듀스).
`arrayJoin` 함수는 각 행을 여러 행으로 펼쳐 행 집합을 생성합니다(언폴드).

이 함수는 배열을 인수로 받아, 배열의 원소 수만큼 원본 행을 여러 행으로 전개합니다.
모든 컬럼의 값은 이 함수가 적용된 컬럼을 제외하면 그대로 복사되며, 해당 컬럼의 값은 각 배열 원소에 대응하는 값으로 대체됩니다.

:::note
배열이 비어 있으면 `arrayJoin`은 아무 행도 생성하지 않습니다.
배열 타입의 기본값을 포함한 단일 행을 반환하려면 [emptyArrayToSingle](./array-functions.md#emptyArrayToSingle)로 감쌀 수 있습니다. 예: `arrayJoin(emptyArrayToSingle(...))`.
:::

예시:

```sql title="Query"
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```text title="Response"
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

`arrayJoin` 함수는 `WHERE` 절을 포함한 쿼리의 모든 부분에 영향을 줍니다. 하위 쿼리가 1개의 행만 반환했는데도 아래 쿼리의 결과가 `2`라는 점에 유의하십시오.

```sql title="Query"
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Babruysk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

```text title="Response"
┌─impressions─┐
│           2 │
└─────────────┘
```

하나의 쿼리에서 여러 개의 `arrayJoin` 함수를 사용할 수 있습니다. 이 경우 변환이 여러 번 수행되어 행 수가 곱절로 늘어납니다.
예시:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Babruysk │ Chrome  │
│           1 │ Babruysk │ Firefox │
└─────────────┴──────────┴─────────┘
```

<div id="important-note">
  ### 권장 사항
</div>

동일한 표현식에 `arrayJoin`을 여러 번 사용하면 공통 부분 표현식이 제거되어 예상한 결과가 나오지 않을 수 있습니다.
이 경우에는 반복되는 배열 표현식에 JOIN 결과에 영향을 주지 않는 추가 연산을 적용하는 방식을 고려하십시오. 예를 들어 `arrayJoin(arraySort(arr))`, `arrayJoin(arrayConcat(arr, []))`

예시:

```sql title="Query"
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

더 다양한 활용이 가능한 SELECT 쿼리의 [`ARRAY JOIN`](../statements/select/array-join.md) 구문에 유의하십시오.
`ARRAY JOIN`을 사용하면 원소 수가 같은 여러 배열을 한 번에 변환할 수 있습니다.

예시:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

또는 [`Tuple`](../data-types/tuple.md)을 사용할 수 있습니다.

예시:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Row"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

ClickHouse에서 `arrayJoin`이라는 이름은 JOIN 연산과 개념적으로 유사하지만, 단일 행 내부의 배열에 적용된다는 점에서 유래했습니다. 전통적인 조인(JOIN)은 서로 다른 테이블의 행을 결합하지만, `arrayJoin`은 한 행의 배열에 있는 각 원소를 &quot;조인&quot;하여 배열 원소마다 하나의 행이 생성되도록 하면서 다른 컬럼 값은 그대로 복제합니다. ClickHouse는 또한 [`ARRAY JOIN`](/ko/sql-reference/statements/select/array-join) 절 구문을 제공하며, 익숙한 SQL JOIN 용어를 사용해 이러한 전통적인 JOIN 연산과의 관계를 더욱 분명하게 보여줍니다. 이 과정은 배열을 &quot;펼친다(unfolding)&quot;라고도 부르지만, 함수 이름과 절 모두에서 &quot;join&quot;이라는 용어를 사용하는 이유는 테이블을 배열 원소와 조인하는 것처럼 보이기 때문이며, 결과적으로 데이터셋을 JOIN 연산과 비슷한 방식으로 확장합니다.