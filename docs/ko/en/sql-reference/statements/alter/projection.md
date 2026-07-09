---
description: '프로젝션 관리 문서'
sidebar_label: 'PROJECTION'
sidebar_position: 49
slug: /sql-reference/statements/alter/projection
title: '프로젝션'
doc_type: '참고'
---

이 페이지에서는 프로젝션의 개념과 사용 방법, 그리고 프로젝션을 관리하는 다양한 옵션을 설명합니다.

<div id="overview">
  ## 프로젝션 개요
</div>

프로젝션은 쿼리 실행을 최적화하는 포맷으로 데이터를 저장하며, 다음과 같은 경우에 유용합니다.

* 프라이머리 키의 일부가 아닌 컬럼에 대해 쿼리를 실행하는 경우
* 컬럼을 사전 집계하여 계산량과 IO를 모두 줄이는 경우

테이블에 하나 이상의 프로젝션을 정의할 수 있으며, 쿼리 분석 중에는 사용자 쿼리를 수정하지 않고도 스캔할 데이터가 가장 적은 프로젝션이 ClickHouse에 의해 선택됩니다.

:::note[디스크 사용량]
프로젝션은 내부적으로 새로운 숨김 테이블(hidden table)을 생성하므로, 더 많은 IO와 디스크 공간이 필요합니다.
예를 들어 프로젝션에 다른 프라이머리 키가 정의되어 있으면, 원본 테이블의 모든 데이터가 복제됩니다.
:::

프로젝션이 내부적으로 어떻게 동작하는지에 관한 더 자세한 기술적 내용은 이 [페이지](/ko/guides/best-practices/sparse-primary-indexes.md/#option-3-projections)에서 확인할 수 있습니다.

<div id="examples">
  ## 프로젝션 사용
</div>

<div id="example-filtering-without-using-primary-keys">
  ### 기본 키(primary key)를 사용하지 않는 필터링 예시
</div>

테이블 생성:

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

`ALTER TABLE`을 사용하면 기존 테이블에 프로젝션을 추가할 수 있습니다:

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

데이터 삽입:

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

Projection을 사용하면 원본 테이블에서 `user_name`이 `PRIMARY_KEY`로 정의되어 있지 않더라도 `user_name`으로 빠르게 필터링할 수 있습니다.
쿼리 시점에 ClickHouse는 데이터가 `user_name` 기준으로 정렬되어 있으므로 프로젝션을 사용할 때 처리해야 할 데이터가 더 적다고 판단합니다.

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

쿼리에서 프로젝션을 사용했는지 확인하려면 `system.query_log` 테이블(table)을 검토할 수 있습니다. `projections` 필드(field)에는 사용된 프로젝션의 이름이 표시되며, 사용된 프로젝션이 없으면 비어 있습니다:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="example-pre-aggregation-query">
  ### 예시 사전 집계 쿼리
</div>

`projection_visits_by_user` 프로젝션이 포함된 테이블을 생성합니다:

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

데이터를 삽입하세요:

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

`user_agent` 필드를 사용해 `GROUP BY`가 포함된 첫 번째 쿼리를 실행합니다.
사전 집계와 일치하지 않으므로 이 쿼리에서는 정의된 프로젝션을 사용하지 않습니다.

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

프로젝션을 활용하려면 사전 집계와 `GROUP BY` 필드의 일부 또는 전체를 선택하는 쿼리를 실행하면 됩니다:

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

앞서 언급했듯이, 프로젝션이 사용되었는지 확인하려면 `system.query_log` 테이블을 검토할 수 있습니다.
`projections` 필드에는 사용된 프로젝션의 이름이 표시됩니다.
프로젝션이 사용되지 않은 경우 비어 있습니다:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="projection-indexes">
  ### 프로젝션 인덱스 생성 및 사용
</div>

[프로젝션 인덱스](../../../engines/table-engines/mergetree-family/mergetree.md#projection-index) 생성 방법:

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">
  <summary>명시적 `_part_offset` 필드를 사용해 프로젝션 생성하기</summary>

  프로젝션 인덱스는 다음 구문으로도 생성할 수 있지만, 권장되지는 않습니다:

  ```sql
  CREATE TABLE events
  (
      `event_time` DateTime,
      `event_id` UInt64,
      `user_id` UInt64,
      `huge_string` String,
      PROJECTION order_by_user_id
      (
          SELECT
              _part_offset
          ORDER BY user_id
      )
  )
  ENGINE = MergeTree()
  ORDER BY (event_id);
  ```
</details>

샘플 데이터를 삽입합니다:

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

`_part_offset` 필드는 머지와 뮤테이션 후에도 값을 유지하므로 보조 인덱싱에 유용합니다. 이를 쿼리에서 활용할 수 있습니다:

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

<div id="example-projection-with-where">
  ### `WHERE` 절을 사용하는 프로젝션 예시
</div>

프로젝션에는 행의 부분 집합만 저장하도록 `WHERE` 절을 포함할 수 있습니다. 이는 쿼리에서 특정 프레디케이트로 자주 필터링하는 경우 유용합니다 — 프로젝션이 일치하는 행만 구체화해 저장하므로 저장 공간을 줄이고 쿼리 성능을 향상시킵니다.

테이블을 생성하고 필터링된 프로젝션을 추가합니다:

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

데이터 삽입:

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

쿼리의 `WHERE` 절이 프로젝션의 `WHERE` 절을 **함의하는 경우**(즉, 프로젝션의 필터에 있는 모든 조건이 쿼리의 필터에도 포함된 경우), 옵티마이저는 이것이 유리하다고 판단하면 프로젝션을 자동으로 사용할 수 있습니다:

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

함의 검사는 보수적으로 수행됩니다 — 정규 표현식 형태에서 연언 항목의 정확한 일치만 사용합니다. 따라서 일부 유효한 최적화 기회(예: 범위 함의)는 놓칠 수 있지만, 잘못된 결과를 생성하지는 않습니다.

<div id="manipulating-projections">
  ## 프로젝션 관리
</div>

[프로젝션](/ko/engines/table-engines/mergetree-family/mergetree.md/#projections)에 대해 다음 작업을 수행할 수 있습니다:

<div id="add-projection">
  ### ADD PROJECTION
</div>

아래 구문을 사용하여 테이블의 메타데이터에 프로젝션 설명을 추가하십시오:

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

:::note
프로젝션에 `WHERE` 절이 정의되어 있으면 프레디케이트를 만족하는 행만 구체화됩니다. 쿼리의 `WHERE`가 논리적으로 프로젝션의 `WHERE`를 함의하고, 해당 프로젝션이 쿼리 계획에 도움이 되는 경우 최적화기는 이런 프로젝션을 사용할 수 있습니다. 이는 일반 프로젝션과 집계 프로젝션 모두에 적용됩니다.
:::

<div id="with-settings">
  #### `WITH SETTINGS` 절
</div>

`WITH SETTINGS`는 **프로젝션 수준의 설정**을 정의하며, 이를 통해 프로젝션이 데이터를 저장하는 방식을 사용자 지정합니다(예: `index_granularity` 또는 `index_granularity_bytes`).
이는 **MergeTree 테이블 설정**과 직접 대응하지만, **이 프로젝션에만** 적용됩니다.

예시:

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

프로젝션 설정은 검증 규칙이 적용되며(예: 잘못되었거나 호환되지 않는 재정의는 거부됩니다), 해당 프로젝션에 실제로 적용되는 테이블 설정을 재정의합니다.

<div id="drop-projection">
  ### DROP PROJECTION
</div>

테이블 메타데이터에서 프로젝션 설명을 제거하고 디스크의 프로젝션 파일을 삭제하려면 아래 SQL 문을 사용합니다.
이 작업은 [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다.

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

<div id="materialize-projection">
  ### MATERIALIZE PROJECTION
</div>

아래 SQL 문을 사용하여 파티션 `partition_name`의 프로젝션 `name`을 다시 생성합니다.
이 작업은 [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다.

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

<div id="clear-projection">
  ### CLEAR PROJECTION
</div>

아래 SQL 문을 사용하면 정의는 제거하지 않은 채 디스크에서 프로젝션 파일을 삭제할 수 있습니다.
이 작업은 [뮤테이션](/ko/sql-reference/statements/alter/index.md#mutations)으로 구현됩니다.

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

`ADD`, `DROP`, `CLEAR` 명령은 메타데이터만 변경하거나 파일만 삭제하므로 경량입니다.
또한 이 명령은 복제되며, ClickHouse Keeper 또는 ZooKeeper를 통해 프로젝션 메타데이터를 동기화합니다.

:::note
프로젝션 조작은 [`*MergeTree`](/ko/engines/table-engines/mergetree-family/mergetree.md) 엔진을 사용하는 테이블( [복제된](/ko/engines/table-engines/mergetree-family/replication.md) 변형 포함)에서만 지원됩니다.
:::

<div id="control-projections-merges">
  ### 프로젝션 머지 동작 제어
</div>

쿼리를 실행하면 ClickHouse는 원본 테이블과 프로젝션 중 어디에서 읽을지 선택합니다.
이 결정은 각 테이블 파트별로 개별적으로 이루어집니다.
ClickHouse는 일반적으로 가능한 한 적은 양의 데이터를 읽으려고 하며, 예를 들어 파트의 기본 키를 샘플링하는 등 읽기에 가장 적합한 파트를 식별하기 위해 몇 가지 기법을 사용합니다.
경우에 따라 원본 테이블 파트에 대응하는 프로젝션 파트가 없을 수 있습니다.
예를 들어 SQL에서 테이블의 프로젝션 생성은 기본적으로 &quot;지연 실행&quot; 방식이므로, 새로 삽입된 데이터에만 영향을 주고 기존 파트는 그대로 유지됩니다.

프로젝션 중 하나에는 이미 사전 계산된 집계 값이 포함되어 있으므로, ClickHouse는 쿼리 런타임에 다시 집계하지 않기 위해 해당 프로젝션 파트에서 읽으려고 합니다. 특정 파트에 대응하는 프로젝션 파트가 없으면 쿼리 실행은 원본 파트로 폴백됩니다.

그렇다면 사소하지 않은 데이터 파트의 백그라운드 머지로 인해 원본 테이블의 행이 단순하지 않은 방식으로 변경되면 어떻게 될까요?
예를 들어, 테이블이 `ReplacingMergeTree` 테이블 엔진을 사용한다고 가정해 보겠습니다.
머지 중 여러 입력 파트에서 동일한 행이 감지되면 가장 최신 행 버전(가장 최근에 삽입된 파트의 행)만 유지되고, 이전 버전은 모두 삭제됩니다.

마찬가지로 테이블이 `AggregatingMergeTree` 테이블 엔진을 사용하는 경우, 머지 작업은 입력 파트의 동일한 행을(기본 키 값을 기준으로) 하나의 행으로 접어 부분 집계 상태를 갱신할 수 있습니다.

ClickHouse v24.8 이전에는 프로젝션 파트가 메인 데이터와 조용히 동기화되지 않은 상태가 되거나, 테이블에 프로젝션이 있으면 데이터베이스가 자동으로 예외를 발생시키므로 업데이트 및 삭제 같은 특정 작업을 아예 실행할 수 없었습니다.

v24.8부터는 새로운 테이블 수준 설정 [`deduplicate_merge_projection_mode`](/ko/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode)으로, 앞서 언급한 단순하지 않은 백그라운드 머지 작업이 원본 테이블의 파트에서 발생할 때의 동작을 제어할 수 있습니다.

삭제 뮤테이션도 원본 테이블의 파트에서 행을 삭제하는 파트 병합 작업의 한 예입니다. v24.7부터는 경량한 삭제에 의해 트리거되는 삭제 뮤테이션의 동작을 제어하는 설정 [`lightweight_mutation_projection_mode`](/ko/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode)도 제공됩니다.

아래는 `deduplicate_merge_projection_mode`와 `lightweight_mutation_projection_mode`에 사용할 수 있는 값입니다:

* `throw` (기본값): 예외가 발생하며, 프로젝션 파트가 동기화되지 않은 상태가 되는 것을 방지합니다.
* `drop`: 영향을 받는 프로젝션 테이블 파트가 삭제됩니다. 영향을 받는 프로젝션 파트에 대한 쿼리는 원본 테이블 파트로 폴백됩니다.
* `rebuild`: 영향을 받는 프로젝션 파트가 원본 테이블 파트의 데이터와 일관성을 유지하도록 다시 빌드됩니다.

<div id="limitations">
  ## 제한 사항
</div>

프로젝션의 `ORDER BY` 절에서는 `ALIAS` 컬럼을 사용할 수 없습니다. 예시는 다음과 같습니다:

```sql
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
--highlight-next-line
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

`ALIAS` 컬럼은 물리적으로 저장되지 않고 쿼리 시점에 동적으로 계산되므로, 정렬 표현식이 평가되는 프로젝션 파트 쓰기 경로에서는 사용할 수 없습니다.

대신 `MATERIALIZED` 컬럼을 사용하거나 표현식을 직접 인라인하십시오:

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

<div id="see-also">
  ## 관련 항목
</div>

* [&quot;머지 중 프로젝션 제어&quot; (블로그 게시글)](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
* [&quot;프로젝션&quot; (가이드)](/ko/data-modeling/projections#using-projections-to-speed-up-UK-price-paid)
* [&quot;materialized view와 프로젝션 비교&quot;](https://clickhouse.com/docs/managing-data/materialized-views-versus-projections)