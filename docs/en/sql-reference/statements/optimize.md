---
toc_priority: 47
toc_title: OPTIMIZE
---

# OPTIMIZE Statement {#misc_operations-optimize}

This query tries to initialize an unscheduled merge of data parts for tables.

!!! warning "Warning"
    `OPTIMIZE` can’t fix the `Too many parts` error.

**Syntax**

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
```

The `OPTMIZE` query is supported for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family, the [MaterializedView](../../engines/table-engines/special/materializedview.md) and the [Buffer](../../engines/table-engines/special/buffer.md) engines. Other table engines aren’t supported.

When `OPTIMIZE` is used with the [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) family of table engines, ClickHouse creates a task for merging and waits for execution on all nodes (if the `replication_alter_partitions_sync` setting is enabled).

-   If `OPTIMIZE` doesn’t perform a merge for any reason, it doesn’t notify the client. To enable notifications, use the [optimize_throw_if_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) setting.
-   If you specify a `PARTITION`, only the specified partition is optimized. [How to set partition expression](../../sql-reference/statements/alter/index.md#alter-how-to-specify-part-expr).
-   If you specify `FINAL`, optimization is performed even when all the data is already in one part. Also merge is forced even if concurrent merges are performed.
-   If you specify `DEDUPLICATE`, then completely identical rows (unless by-clause is specified) will be deduplicated (all columns are compared), it makes sense only for the MergeTree engine.


## BY expression {#by-expression}

If you want to perform deduplication on custom set of columns rather than on all, you can specify list of columns explicitly or use any combination of [`*`](../../sql-reference/statements/select/index.md#asterisk), [`COLUMNS`](../../sql-reference/statements/select/index.md#columns-expression) or [`EXCEPT`](../../sql-reference/statements/select/index.md#except-modifier) expressions. The explictly written or implicitly expanded list of columns must include all columns specified in row ordering expression (both primary and sorting keys) and partitioning expression (partitioning key).

!!! note "Note"
    Notice that `*` behaves just like in `SELECT`: `MATERIALIZED` and `ALIAS` columns are not used for expansion.
    Also, it is an error to specify empty list of columns, or write an expression that results in an empty list of columns, or deduplicate by an ALIAS column.

``` sql
OPTIMIZE TABLE table DEDUPLICATE; -- the old one
OPTIMIZE TABLE table DEDUPLICATE BY *; -- not the same as the old one, excludes MATERIALIZED columns (see the note above)
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY col1,col2,col3;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**Examples**

Create a table:

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

The 'old' deduplicate, all columns are taken into account, i.e. row is removed only if all values in all columns are equal to corresponding values in previous row.

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

Deduplicate by all columns that are not `ALIAS` or `MATERIALIZED`: `primary_key`, `secondary_key`, `value`, `partition_key`, and `materialized_value` columns.

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

Deduplicate by all columns that are not `ALIAS` or `MATERIALIZED` and explicitly not `materialized_value`: `primary_key`, `secondary_key`, `value`, and `partition_key` columns.

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT materialized_value;
```

Deduplicate explicitly by `primary_key`, `secondary_key`, and `partition_key` columns.
``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

Deduplicate by any column matching a regex: `primary_key`, `secondary_key`, and `partition_key` columns.

``` sql
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```
