---
description: 'Optimize 문서'
sidebar_label: 'OPTIMIZE'
sidebar_position: 47
slug: /sql-reference/statements/optimize
title: 'OPTIMIZE SQL 문'
doc_type: 'reference'
---

이 쿼리는 테이블의 데이터 파트에 대해 예약되지 않은 머지를 시작하려고 시도합니다. 일반적으로 `OPTIMIZE TABLE ... FINAL`의 사용은 권장하지 않습니다([문서](/ko/optimize/avoidoptimizefinal) 참조). 이 기능은 일상적인 운영이 아니라 관리 작업을 위한 용도이기 때문입니다.

:::note
`OPTIMIZE`는 `Too many parts` 오류를 해결할 수 없습니다.
:::

**구문**

```sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL | FORCE] [DEDUPLICATE [BY expression]]
```

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

`OPTIMIZE` 쿼리는 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 계열([구체화된 뷰(Materialized View)](/ko/sql-reference/statements/create/view#materialized-view) 포함)과 [Buffer](../../engines/table-engines/special/buffer.md) 엔진에서 지원됩니다. 다른 테이블 엔진은 지원되지 않습니다.

`OPTIMIZE`를 [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) 계열의 테이블 엔진과 함께 사용하면 ClickHouse는 머지를 위한 작업을 생성하고, 모든 레플리카에서 실행될 때까지([alter&#95;sync](/ko/operations/settings/settings#alter_sync) 설정이 `2`인 경우) 또는 현재 레플리카에서 실행될 때까지([alter&#95;sync](/ko/operations/settings/settings#alter_sync) 설정이 `1`인 경우) 기다립니다.

* 어떤 이유로든 `OPTIMIZE`가 머지를 수행하지 않으면 클라이언트에 알리지 않습니다. 알림을 활성화하려면 [optimize&#95;throw&#95;if&#95;noop](/ko/operations/settings/settings#optimize_throw_if_noop) 설정을 사용하십시오.
* `PARTITION`을 지정하면 지정한 파티션만 최적화됩니다. [파티션 표현식을 설정하는 방법](alter/partition.md#how-to-set-partition-expression).
* `FINAL` 또는 `FORCE`를 지정하면 모든 데이터가 이미 하나의 파트에 있더라도 최적화가 수행됩니다. 이 동작은 [optimize&#95;skip&#95;merged&#95;partitions](/ko/operations/settings/settings#optimize_skip_merged_partitions) 설정으로 제어할 수 있습니다. 또한 동시 머지가 진행 중인 경우에도 머지가 강제로 수행됩니다.
* `DEDUPLICATE`를 지정하면 완전히 동일한 행은(`by` 절을 지정하지 않은 경우) 중복 제거됩니다(모든 컬럼 비교). 이는 MergeTree 엔진에서만 의미가 있습니다.

[replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ko/operations/settings/settings#replication_wait_for_inactive_replica_timeout) 설정으로 비활성 레플리카가 `OPTIMIZE` 쿼리를 실행할 때까지 기다릴 시간(초)을 지정할 수 있습니다.

:::note
`alter_sync`가 `2`로 설정되어 있고 일부 레플리카가 `replication_wait_for_inactive_replica_timeout` 설정에 지정된 시간보다 오래 비활성 상태인 경우 `UNFINISHED` 예외가 발생합니다.
:::

<div id="dry-run">
  ## DRY RUN
</div>

`DRY RUN` 절은 지정된 파트의 머지를 결과를 커밋하지 않은 채 시뮬레이션합니다. 병합된 파트는 임시 위치에 기록된 후 검증되고, затем 폐기됩니다. 원본 파트와 테이블 데이터는 변경되지 않습니다.

다음과 같은 경우에 유용합니다:

* ClickHouse 버전 간 머지 정확성을 테스트할 때
* 머지 관련 버그를 결정적으로 재현할 때
* 머지 성능을 벤치마크할 때

`DRY RUN`은 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 계열 테이블에서만 지원됩니다. 파트 이름 목록과 함께 `PARTS` 키워드를 반드시 지정해야 합니다. 지정된 모든 파트는 존재해야 하고, 활성 상태여야 하며, 동일한 파티션에 속해야 합니다.

`DRY RUN`은 `FINAL` 및 `PARTITION`과 함께 사용할 수 없습니다. `DEDUPLICATE`(선택적으로 컬럼 지정 가능) 및 `CLEANUP`(`ReplacingMergeTree` 테이블용)과는 함께 사용할 수 있습니다.

**구문**

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

기본적으로 결과로 생성된 병합된 파트는 [`CHECK TABLE`](/ko/sql-reference/statements/check-table) 쿼리와 유사한 방식으로 검증됩니다. 이 동작은 [optimize&#95;dry&#95;run&#95;check&#95;part](/ko/operations/settings/settings#optimize_dry_run_check_part) 설정으로 제어되며(기본적으로 활성화되어 있음), 이를 비활성화하면 검증을 건너뜁니다. 이는 머지 작업 자체를 벤치마크할 때 유용할 수 있습니다.

**예시**

```sql
CREATE TABLE dry_run_example (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO dry_run_example VALUES (1, 'a'), (2, 'b');
INSERT INTO dry_run_example VALUES (1, 'c'), (4, 'd');

-- Simulate merging using two parts
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0';

-- Simulate merging with deduplication
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' DEDUPLICATE;

-- Parts and data remain unchanged after DRY RUN
SELECT name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'dry_run_example' AND active
ORDER BY name;
```

```response
┌─name────────┬─rows─┐
│ all_1_1_0   │    2 │
│ all_2_2_0   │    2 │
└─────────────┴──────┘
```

<div id="by-expression">
  ## BY 표현식
</div>

모든 컬럼이 아닌 특정 컬럼 집합에 대해 중복 제거를 수행하려면, 컬럼 목록을 명시적으로 지정하거나 [`*`](../../sql-reference/statements/select/index.md#asterisk), [`COLUMNS`](/ko/sql-reference/statements/select#select-clause), [`EXCEPT`](/ko/sql-reference/statements/select/except-modifier) 표현식을 조합해 사용할 수 있습니다. 명시적으로 작성했거나 암묵적으로 확장된 컬럼 목록에는 행 정렬 표현식(프라이머리 키와 정렬 키 모두) 및 파티셔닝 표현식(파티셔닝 키)에 지정된 모든 컬럼이 포함되어야 합니다.

:::note
`*`는 `SELECT`에서와 마찬가지로 동작합니다. 즉, 확장할 때 [MATERIALIZED](/ko/sql-reference/statements/create/view#materialized-view) 및 [ALIAS](../../sql-reference/statements/create/table.md#alias) 컬럼은 사용되지 않습니다.

또한 빈 컬럼 목록을 지정하거나, 결과적으로 빈 컬럼 목록이 되는 표현식을 작성하거나, `ALIAS` 컬럼을 기준으로 중복 제거를 수행하는 것은 오류입니다.
:::

**구문**

```sql
OPTIMIZE TABLE table DEDUPLICATE; -- all columns
OPTIMIZE TABLE table DEDUPLICATE BY *; -- excludes MATERIALIZED and ALIAS columns
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**예시**

다음 테이블을 살펴보겠습니다:

```sql title="Query"
CREATE TABLE example (
    primary_key Int32,
    secondary_key Int32,
    value UInt32,
    partition_key UInt32,
    materialized_value UInt32 MATERIALIZED 12345,
    aliased_value UInt32 ALIAS 2,
    PRIMARY KEY primary_key
) ENGINE=MergeTree
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);
```

```sql title="Query"
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```

```sql title="Query"
SELECT * FROM example;
```

```sql title="Response"

┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

다음의 모든 예시는 5개의 행이 있는 이 상태를 대상으로 실행됩니다.

<div id="deduplicate">
  #### `DEDUPLICATE`
</div>

중복 제거에 사용할 컬럼을 지정하지 않으면 모든 컬럼이 대상이 됩니다. 이전 행의 해당 값과 모든 컬럼의 값이 모두 같을 때만 행이 제거됩니다:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-">
  #### `DEDUPLICATE BY *`
</div>

컬럼이 암시적으로 지정되면, 테이블은 `ALIAS` 또는 `MATERIALIZED`가 아닌 모든 컬럼을 기준으로 중복 제거됩니다. 위 표의 경우 해당 컬럼은 `primary_key`, `secondary_key`, `value`, `partition_key`입니다:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by--except">
  #### `DEDUPLICATE BY * EXCEPT`
</div>

`ALIAS` 또는 `MATERIALIZED`가 아니며, `value`를 명시적으로 제외한 모든 컬럼, 즉 `primary_key`, `secondary_key`, `partition_key` 컬럼을 기준으로 중복을 제거합니다.

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-list-of-columns">
  #### `DEDUPLICATE BY <list of columns>`
</div>

`primary_key`, `secondary_key`, `partition_key` 컬럼을 명시적으로 지정하여 중복을 제거합니다:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-columnsregex">
  #### `DEDUPLICATE BY COLUMNS(<regex>)`
</div>

정규식과 일치하는 모든 컬럼, 즉 `primary_key`, `secondary_key`, `partition_key` 컬럼을 기준으로 중복을 제거합니다:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```