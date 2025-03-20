---
slug: /en/sql-reference/statements/optimize
sidebar_position: 47
sidebar_label: OPTIMIZE
title: "OPTIMIZE Statement"
---

This query tries to initialize an unscheduled merge of data parts for tables. Note that we generally recommend against using `OPTIMIZE TABLE ... FINAL` (see these [docs](/docs/en/optimize/avoidoptimizefinal)) as its use case is meant for administration, not for daily operations.

:::note
`OPTIMIZE` can’t fix the `Too many parts` error.
:::

**Syntax**

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
```

The `OPTIMIZE` query is supported for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family (including [materialized views](../../sql-reference/statements/create/view.md#materialized-view)) and the [Buffer](../../engines/table-engines/special/buffer.md) engines. Other table engines aren’t supported.

When `OPTIMIZE` is used with the [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) family of table engines, ClickHouse creates a task for merging and waits for execution on all replicas (if the [alter_sync](../../operations/settings/settings.md#alter-sync) setting is set to `2`) or on current replica (if the [alter_sync](../../operations/settings/settings.md#alter-sync) setting is set to `1`).

- If `OPTIMIZE` does not perform a merge for any reason, it does not notify the client. To enable notifications, use the [optimize_throw_if_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) setting.
- If you specify a `PARTITION`, only the specified partition is optimized. [How to set partition expression](alter/partition.md#how-to-set-partition-expression).
- If you specify `FINAL`, optimization is performed even when all the data is already in one part. You can control this behaviour with [optimize_skip_merged_partitions](../../operations/settings/settings.md#optimize-skip-merged-partitions). Also, the merge is forced even if concurrent merges are performed.
- If you specify `DEDUPLICATE`, then completely identical rows (unless by-clause is specified) will be deduplicated (all columns are compared), it makes sense only for the MergeTree engine.

You can specify how long (in seconds) to wait for inactive replicas to execute `OPTIMIZE` queries by the [replication_wait_for_inactive_replica_timeout](../../operations/settings/settings.md#replication-wait-for-inactive-replica-timeout) setting.

:::note    
If the `alter_sync` is set to `2` and some replicas are not active for more than the time, specified by the `replication_wait_for_inactive_replica_timeout` setting, then an exception `UNFINISHED` is thrown.
:::

## BY expression

If you want to perform deduplication on custom set of columns rather than on all, you can specify list of columns explicitly or use any combination of [`*`](../../sql-reference/statements/select/index.md#asterisk), [`COLUMNS`](../../sql-reference/statements/select/index.md#columns-expression) or [`EXCEPT`](../../sql-reference/statements/select/index.md#except-modifier) expressions. The explicitly written or implicitly expanded list of columns must include all columns specified in row ordering expression (both primary and sorting keys) and partitioning expression (partitioning key).

:::note    
Notice that `*` behaves just like in `SELECT`: [MATERIALIZED](../../sql-reference/statements/create/table.md#materialized) and [ALIAS](../../sql-reference/statements/create/table.md#alias) columns are not used for expansion.

Also, it is an error to specify empty list of columns, or write an expression that results in an empty list of columns, or deduplicate by an `ALIAS` column.
:::

**Syntax**

``` sql
OPTIMIZE TABLE table DEDUPLICATE; -- all columns
OPTIMIZE TABLE table DEDUPLICATE BY *; -- excludes MATERIALIZED and ALIAS columns
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**Examples**

Consider the table:

``` sql
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
``` sql
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```
``` sql
SELECT * FROM example;
```
Result:
```

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
All following examples are executed against this state with 5 rows.

#### `DEDUPLICATE`
When columns for deduplication are not specified, all of them are taken into account. The row is removed only if all values in all columns are equal to corresponding values in the previous row:

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```
``` sql
SELECT * FROM example;
```
Result:
```
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
#### `DEDUPLICATE BY *`
When columns are specified implicitly, the table is deduplicated by all columns that are not `ALIAS` or `MATERIALIZED`. Considering the table above, these are `primary_key`, `secondary_key`, `value`, and `partition_key` columns:
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```
``` sql
SELECT * FROM example;
```
Result:
```
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
#### `DEDUPLICATE BY * EXCEPT`
Deduplicate by all columns that are not `ALIAS` or `MATERIALIZED` and explicitly not `value`: `primary_key`, `secondary_key`, and `partition_key` columns.

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```
``` sql
SELECT * FROM example;
```
Result:
```
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
#### `DEDUPLICATE BY <list of columns>`
Deduplicate explicitly by `primary_key`, `secondary_key`, and `partition_key` columns:
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```
``` sql
SELECT * FROM example;
```
Result:
```
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
#### `DEDUPLICATE BY COLUMNS(<regex>)`
Deduplicate by all columns matching a regex: `primary_key`, `secondary_key`, and `partition_key` columns:
```sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```
``` sql
SELECT * FROM example;
```
Result:
```
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
