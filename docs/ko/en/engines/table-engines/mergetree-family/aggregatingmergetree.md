---
description: '동일한 프라이머리 키(primary key)(더 정확히는 동일한 [정렬 키(sorting key)](../../../engines/table-engines/mergetree-family/mergetree.md))를 가진 모든 행을
  집계 함수 상태의 조합을 저장하는 단일 행으로
  대체합니다(단일 데이터 파트 내에서).'
sidebar_label: 'AggregatingMergeTree'
sidebar_position: 60
slug: /engines/table-engines/mergetree-family/aggregatingmergetree
title: 'AggregatingMergeTree 테이블 엔진'
doc_type: '참고'
---

이 엔진은 [MergeTree](/ko/engines/table-engines/mergetree-family/mergetree)를 상속하며, 데이터 파트 머지 로직을 변경합니다. ClickHouse는 동일한 프라이머리 키(primary key)(더 정확히는 동일한 [정렬 키(sorting key)](../../../engines/table-engines/mergetree-family/mergetree.md))를 가진 모든 행을 집계 함수 상태의 조합을 저장하는 단일 행으로 대체합니다(단일 데이터 파트 내에서).

`AggregatingMergeTree` 테이블은 집계된 materialized view를 포함해 증분 데이터 집계에 사용할 수 있습니다.

아래 동영상에서 AggregatingMergeTree와 집계 함수의 사용 예시를 확인할 수 있습니다:

<div class="vimeo-container">
  <iframe width="1030" height="579" src="https://www.youtube.com/embed/pryhI4F_zqQ" title="ClickHouse의 집계 상태" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />
</div>

이 엔진은 다음 타입의 모든 컬럼을 처리합니다:

* [`AggregateFunction`](../../../sql-reference/data-types/aggregatefunction.md)
* [`SimpleAggregateFunction`](../../../sql-reference/data-types/simpleaggregatefunction.md)

`AggregatingMergeTree`는 행 수를 여러 자릿수만큼 줄일 수 있을 때 사용하는 것이 적절합니다.

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

요청 매개변수에 대한 설명은 [요청 설명](../../../sql-reference/statements/create/table.md)을 참조하십시오.

**쿼리 절**

`AggregatingMergeTree` 테이블을 생성할 때는 `MergeTree` 테이블을 생성할 때와 동일한 [절](../../../engines/table-engines/mergetree-family/mergetree.md)이 필요합니다.

<details markdown="1">
  <summary>Deprecated 테이블 생성 메서드</summary>

  :::note
  새 프로젝트에서는 이 메서드를 사용하지 마십시오. 가능하면 기존 프로젝트도 위에서 설명한 메서드로 전환하십시오.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  모든 매개변수의 의미는 `MergeTree`와 동일합니다.
</details>

<div id="select-and-insert">
  ## SELECT 및 INSERT
</div>

데이터를 삽입하려면 집계 -State- 함수와 함께 [INSERT SELECT](../../../sql-reference/statements/insert-into.md) 쿼리를 사용합니다.
`AggregatingMergeTree` 테이블에서 데이터를 조회할 때는 `GROUP BY` 절과 데이터 삽입 시 사용한 것과 동일한 집계 함수를 사용하되, `-Merge` 접미사를 붙여야 합니다.

`SELECT` 쿼리 결과에서 `AggregateFunction` 타입의 값은 모든 ClickHouse 출력 형식에 대해 구현에 따라 달라지는 이진 표현을 가집니다. 예를 들어, `SELECT` 쿼리로 데이터를 `TabSeparated` 포맷으로 덤프하면, 이 덤프를 `INSERT` 쿼리로 다시 로드할 수 있습니다.

<div id="example-of-an-aggregated-materialized-view">
  ## 집계된 materialized view 예시
</div>

다음 예시는 `test`라는 데이터베이스(database)가 있다고 가정합니다. 아직 없다면 아래 명령으로 생성하십시오:

```sql
CREATE DATABASE test;
```

이제 원시 데이터가 들어 있는 테이블 `test.visits`를 생성하세요:

```sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

다음으로, 총 방문 수와 고유 사용자 수를 추적하는 `AggregationFunction`을 저장할 `AggregatingMergeTree` 테이블이 필요합니다.

`test.visits` 테이블을 대상으로 하고 [`AggregateFunction`](/ko/sql-reference/data-types/aggregatefunction) 타입을 사용하는 `AggregatingMergeTree` materialized view를 생성합니다:

```sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

`test.visits`에서 `test.agg_visits`를 채우도록 materialized view를 생성합니다:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

`test.visits` 테이블에 데이터를 삽입하세요:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

데이터는 `test.visits`와 `test.agg_visits` 양쪽에 모두 삽입됩니다.

집계된 데이터를 얻으려면 materialized view `test.visits_mv`에 대해 `SELECT ... GROUP BY ...`와 같은 쿼리를 실행합니다:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.visits_mv
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

`test.visits`에 레코드 2개를 더 추가하되, 이번에는 그중 하나에 다른 timestamp를 사용해 보십시오:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

`SELECT` 쿼리를 다시 실행하면 다음과 같은 출력이 반환됩니다:

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

경우에 따라 집계 비용을 삽입 시점에서
머지 시점으로 옮기기 위해, 삽입 시점에 행을 미리 집계하지 않도록 할 수 있습니다. 일반적으로는 오류를 방지하려면 materialized view 정의의 `GROUP BY`
절에 집계에 포함되지 않은 컬럼을 포함해야 합니다. 하지만 [`initializeAggregation`](/ko/sql-reference/functions/other-functions#initializeAggregation)
함수와 `optimize_on_insert = 0` 설정(기본적으로 활성화됨)을 사용하면 이를 구현할 수 있습니다. 이 경우에는 `GROUP BY`
가 더 이상 필요하지 않습니다:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    initializeAggregation('sumState', Sign) AS Visits,
    initializeAggregation('uniqState', UserID) AS Users
FROM test.visits;
```

:::note
`initializeAggregation`을 사용하면 그룹화 없이 각 개별 행에 대해 집계 상태가 생성됩니다.
각 원본 행은 materialized view에서 하나의 행을 생성하고, 실제 집계는 이후
`AggregatingMergeTree`가 파트를 머지할 때 이루어집니다. 이는 `optimize_on_insert = 0`인 경우에만 적용됩니다.
:::

<div id="tuple-element-aggregation">
  ## Tuple 요소 집계
</div>

`allow_tuple_element_aggregation` 설정이 활성화되면 `Tuple` 컬럼은 재귀적으로 평탄화되어 각 리프 요소가 서로 독립적으로 집계에 참여합니다. 즉, `Tuple` 내부의 `AggregateFunction` 또는 `SimpleAggregateFunction` 서브컬럼은 최상위 컬럼인 경우와 마찬가지로 각 함수에 따라 집계됩니다.

정렬 키에 속한 `Tuple`의 서브컬럼은 집계에서 제외됩니다. 집계 함수가 아닌 서브컬럼은 일반 컬럼처럼 처리되며, 첫 번째 값이 유지됩니다.

:::note
이 설정은 변경할 수 없으며 테이블 생성 시점에 반드시 지정해야 합니다.
:::

```sql
CREATE TABLE agg_tuples
(
    key UInt32,
    metrics Tuple(
        total_visits SimpleAggregateFunction(sum, UInt64),
        unique_users SimpleAggregateFunction(max, UInt64)
    )
) ENGINE = AggregatingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO agg_tuples VALUES (1, (100, 5));
INSERT INTO agg_tuples VALUES (1, (200, 8));
INSERT INTO agg_tuples VALUES (2, (50, 3));

OPTIMIZE TABLE agg_tuples FINAL;

SELECT key, metrics.total_visits, metrics.unique_users FROM agg_tuples ORDER BY key;
```

```text
┌─key─┬─metrics.total_visits─┬─metrics.unique_users─┐
│   1 │                  300 │                    8 │
│   2 │                   50 │                    3 │
└─────┴──────────────────────┴──────────────────────┘
```

`total_visits`는 `sum`으로 집계되고(100 + 200 = 300), `unique_users`는 `max`로 집계됩니다(max(5, 8) = 8).

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 배열, 맵, 상태에 Aggregate Combinator 활용하기](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)