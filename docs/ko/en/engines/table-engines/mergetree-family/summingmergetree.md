---
description: 'SummingMergeTree는 MergeTree 엔진을 상속합니다. 주요 기능은 파트 병합 중 숫자 데이터 타입을 자동으로 합산하는 것입니다.'
sidebar_label: 'SummingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/summingmergetree
title: 'SummingMergeTree 테이블 엔진'
doc_type: 'reference'
---

이 엔진은 [MergeTree](/ko/engines/table-engines/mergetree-family/mergetree)를 상속합니다. 차이점은 `SummingMergeTree` 테이블의 데이터 파트를 머지할 때 ClickHouse가 동일한 프라이머리 키(primary key)(더 정확히는 동일한 [정렬 키(sorting key)](../../../engines/table-engines/mergetree-family/mergetree.md))를 가진 모든 행을, 숫자 데이터 타입 컬럼의 값이 합산된 하나의 행으로 대체한다는 점입니다. 정렬 키가 하나의 키 값에 많은 수의 행이 대응되도록 구성되어 있으면 저장소 용량이 크게 줄어들고 데이터 조회 속도도 빨라집니다.

이 엔진은 `MergeTree`와 함께 사용하는 것을 권장합니다. 전체 데이터는 `MergeTree` 테이블에 저장하고, 예를 들어 보고서 작성 시에는 집계된 데이터를 저장하는 용도로 `SummingMergeTree`를 사용하십시오. 이렇게 하면 프라이머리 키를 잘못 구성해 중요한 데이터를 잃는 일을 방지할 수 있습니다.

<div id="creating-a-table">
  ## 테이블 생성하기
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

요청 매개변수에 대한 자세한 내용은 [요청 설명](../../../sql-reference/statements/create/table.md)을 참조하십시오.

<div id="parameters-of-summingmergetree">
  ### SummingMergeTree 매개변수
</div>

<div id="columns">
  #### 컬럼
</div>

`columns` - 값이 합산될 컬럼 이름의 튜플입니다. 선택적 매개변수입니다.
해당 컬럼은 모두 숫자형 데이터 타입이어야 하며, 파티션 또는 정렬 키에 포함되어서는 안 됩니다.

`columns`를 지정하지 않으면, ClickHouse는 정렬 키에 포함되지 않은 모든 숫자형 데이터 타입 컬럼의 값을 합산합니다.

<div id="query-clauses">
  ### 쿼리 절
</div>

`SummingMergeTree` 테이블을 생성할 때는 `MergeTree` 테이블을 생성할 때와 동일한 [절](../../../engines/table-engines/mergetree-family/mergetree.md)이 필요합니다.

<details markdown="1">
  <summary>더 이상 권장되지 않는 테이블 생성 메서드</summary>

  :::note
  새 프로젝트에서는 이 메서드를 사용하지 말고, 가능하면 기존 프로젝트도 위에서 설명한 메서드로 전환하십시오.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  `columns`를 제외한 모든 매개변수의 의미는 `MergeTree`와 동일합니다.

  * `columns` — 값이 합산될 컬럼 이름의 튜플입니다. 선택적 매개변수입니다. 자세한 내용은 위 설명을 참조하십시오.
</details>

<div id="usage-example">
  ## 사용 예시
</div>

다음 테이블을 살펴보겠습니다:

```sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

여기에 데이터를 삽입하세요:

```sql
INSERT INTO summtt VALUES(1,1),(1,2),(2,1)
```

ClickHouse에서는 모든 행이 완전히 합산되지 않을 수 있으므로([아래 참조](#data-processing)), 쿼리에서 집계 함수 `sum`과 `GROUP BY` 절을 사용합니다.

```sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

```text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

<div id="data-processing">
  ## 데이터 처리
</div>

데이터가 테이블에 삽입되면 그대로 저장됩니다. ClickHouse는 삽입된 데이터 파트를 주기적으로 머지하며, 이 과정에서 동일한 기본 키(primary key)를 가진 행은 합산되어 각 결과 데이터 파트에서 하나의 행으로 합쳐집니다.

ClickHouse는 데이터 파트를 머지할 때, 서로 다른 결과 데이터 파트에 동일한 기본 키를 가진 행이 남을 수 있으므로 합산이 완전하게 이루어지지 않을 수 있습니다. 따라서 위 예시에서 설명한 것처럼 쿼리에서는 집계 함수(aggregate function) [sum()](/ko/sql-reference/aggregate-functions/reference/sum)와 `GROUP BY` 절을 사용해야 합니다.

<div id="common-rules-for-summation">
  ### 합산의 일반 규칙
</div>

숫자형 데이터 타입을 가진 컬럼의 값은 합산됩니다. 컬럼 집합은 매개변수 `columns`로 정의됩니다.

합산 대상인 모든 컬럼의 값이 0이면 해당 행은 삭제됩니다.

컬럼이 프라이머리 키(primary key)에 포함되지 않고 합산 대상도 아닌 경우에는, 기존 값 중 임의의 값이 선택됩니다.

프라이머리 키에 포함된 컬럼의 값은 합산되지 않습니다.

<div id="the-summation-in-the-aggregatefunction-columns">
  ### AggregateFunction 컬럼에서의 합산
</div>

[AggregateFunction type](../../../sql-reference/data-types/aggregatefunction.md) 타입의 컬럼에서 ClickHouse는 함수에 따라 집계를 수행하는 [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 엔진처럼 동작합니다.

<div id="nested-structures">
  ### 중첩 구조
</div>

테이블에는 특별한 방식으로 처리되는 중첩 데이터 구조가 있을 수 있습니다.

중첩 테이블의 이름이 `Map`으로 끝나고, 다음 기준을 충족하는 컬럼이 2개 이상 포함되어 있으면:

* 첫 번째 컬럼은 숫자형 `(*Int*, Date, DateTime)` 또는 문자열 `(String, FixedString)`이며, 이를 `key`라고 합니다.
* 나머지 컬럼은 산술형 `(*Int*, Float32/64)`이며, 이를 `(values...)`라고 합니다.

이 경우 이 중첩 테이블은 `key => (values...)`에 대한 매핑으로 해석되며, 행을 머지할 때 두 데이터 집합의 요소는 `key`를 기준으로 머지되고 해당 `(values...)`는 합산됩니다.

예시:

```text
DROP TABLE IF EXISTS nested_sum;
CREATE TABLE nested_sum
(
    date Date,
    site UInt32,
    hitsMap Nested(
        browser String,
        imps UInt32,
        clicks UInt32
    )
) ENGINE = SummingMergeTree
PRIMARY KEY (date, site);

INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Firefox', 'Opera'], [10, 5], [2, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Chrome', 'Firefox'], [20, 1], [1, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['IE'], [22], [0]);
INSERT INTO nested_sum VALUES ('2020-01-01', 10, ['Chrome'], [4], [3]);

OPTIMIZE TABLE nested_sum FINAL; -- emulate merge 

SELECT * FROM nested_sum;
┌───────date─┬─site─┬─hitsMap.browser───────────────────┬─hitsMap.imps─┬─hitsMap.clicks─┐
│ 2020-01-01 │   10 │ ['Chrome']                        │ [4]          │ [3]            │
│ 2020-01-01 │   12 │ ['Chrome','Firefox','IE','Opera'] │ [20,11,22,5] │ [1,3,0,1]      │
└────────────┴──────┴───────────────────────────────────┴──────────────┴────────────────┘

SELECT
    site,
    browser,
    impressions,
    clicks
FROM
(
    SELECT
        site,
        sumMap(hitsMap.browser, hitsMap.imps, hitsMap.clicks) AS imps_map
    FROM nested_sum
    GROUP BY site
)
ARRAY JOIN
    imps_map.1 AS browser,
    imps_map.2 AS impressions,
    imps_map.3 AS clicks;

┌─site─┬─browser─┬─impressions─┬─clicks─┐
│   12 │ Chrome  │          20 │      1 │
│   12 │ Firefox │          11 │      3 │
│   12 │ IE      │          22 │      0 │
│   12 │ Opera   │           5 │      1 │
│   10 │ Chrome  │           4 │      3 │
└──────┴─────────┴─────────────┴────────┘
```

데이터를 조회할 때는 `Map`을 집계하기 위해 [sumMap(key, value)](../../../sql-reference/aggregate-functions/reference/sumMappedArrays.md) 함수를 사용합니다.

중첩 데이터 구조는 합산을 위한 컬럼 튜플에 해당 컬럼들을 지정할 필요가 없습니다.

<div id="tuple-element-aggregation">
  ### Tuple 요소 집계
</div>

`allow_tuple_element_aggregation` 설정이 활성화되면 `Tuple` 컬럼은 각 리프 요소가 개별적으로 합산에 참여할 수 있도록 재귀적으로 평탄화됩니다. 따라서 여러 메트릭을 하나의 `Tuple` 컬럼에 저장하고, 머지 중에 요소별로 합산되도록 할 수 있습니다.

평탄화된 서브컬럼에도 일반 컬럼과 동일한 규칙이 적용됩니다:

* 숫자형 서브컬럼만 합산됩니다.
* 정렬 키(sorting key) 또는 파티션 키(partition key)에 포함된 `Tuple`의 서브컬럼은 합산에서 제외됩니다.
* `columns`가 지정된 경우, 나열된 `Tuple` 컬럼의 서브컬럼만 합산됩니다.
* 합산 후 한 행의 모든 숫자형 서브컬럼이 0이면 해당 행은 삭제됩니다.

:::note
이 설정은 변경할 수 없으며 테이블 생성 시점에 반드시 지정해야 합니다.
:::

```sql
CREATE TABLE summing_tuples
(
    key UInt32,
    metrics Tuple(
        impressions UInt64,
        clicks UInt64,
        nested Tuple(
            conversions UInt64
        )
    )
) ENGINE = SummingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO summing_tuples VALUES (1, (100, 10, (1)));
INSERT INTO summing_tuples VALUES (1, (200, 20, (3)));

OPTIMIZE TABLE summing_tuples FINAL;

SELECT key, metrics.impressions, metrics.clicks, metrics.nested.conversions FROM summing_tuples;
```

```text
┌─key─┬─metrics.impressions─┬─metrics.clicks─┬─metrics.nested.conversions─┐
│   1 │                 300 │             30 │                          4 │
└─────┴─────────────────────┴────────────────┴────────────────────────────┘
```

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 배열, 맵, state에 집계 조합자 사용하기](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)