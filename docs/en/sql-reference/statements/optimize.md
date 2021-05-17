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
    Notice that `*` behaves just like in `SELECT`: [`MATERIALIZED`](../../sql-reference/statements/create/table.md#materialized) and [`ALIAS`](../../sql-reference/statements/create/table.md#alias) columns are not used for expansion.
    Also, it is an error to specify empty list of columns, or write an expression that results in an empty list of columns, or deduplicate by an `ALIAS` column.

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
DROP TABLE IF EXISTS dup_example;

CREATE TABLE dup_example (
    pk Int32, -- primary key
    sk Int32, -- secondary key
    value UInt32,
    mat UInt32 MATERIALIZED rand(),  -- materialized value
    alias UInt32 ALIAS 2, -- aliased value
    PRIMARY KEY pk
) ENGINE=MergeTree
ORDER BY (pk, sk);
```

The `MergeTree` engine does not have parameters.

**Valid cases**

In the case below all columns are taken into account, i.e. row is removed only if all values in all columns are equal to corresponding values in another row.
Here and below we need to add `FINAL` to force deduplication in case of a small set of data.

``` sql
OPTIMIZE TABLE dup_example FINAL DEDUPLICATE;
```

Deduplicate by all columns that are not `ALIAS` or `MATERIALIZED`: in our case deduplicate by `pk`, `sk` and `value` columns.

``` sql
OPTIMIZE TABLE dup_example FINAL DEDUPLICATE BY *;
```

Deduplicate explicitly by `pk`, `sk`, `value` and `mat` columns.
``` sql
OPTIMIZE TABLE dup_example FINAL DEDUPLICATE BY pk, sk, value, mat;
```

Deduplicate by columns matching a regex `'.*k'`: `pk` and `sk` columns.

``` sql
OPTIMIZE TABLE dup_example FINAL DEDUPLICATE BY COLUMNS('.*k');
```

**Error cases**

Note that **primary key** column should not be missed in any `BY` expression. These queries will face errors:

``` sql
OPTIMIZE TABLE dup_example DEDUPLICATE BY * EXCEPT(pk);
OPTIMIZE TABLE dup_example DEDUPLICATE BY sk, value;
```

Empty list cases:
``` sql
OPTIMIZE TABLE dup_example DEDUPLICATE BY * EXCEPT(pk, sk, value, mat, alias); -- server error
OPTIMIZE TABLE dup_example DEDUPLICATE BY; -- syntax error
```

2. Replicated example on a [`ReplicatedMergeTree`](../../engines/table-engines/mergetree-family/replication.md) table engine:

```sql
DROP TABLE IF EXISTS replicated_deduplicate_by_columns_r1;
DROP TABLE IF EXISTS replicated_deduplicate_by_columns_r2;

SET replication_alter_partitions_sync = 2;

CREATE TABLE IF NOT EXISTS replicated_deduplicate_by_columns_r1 (
    id Int32, 
    value UInt32, 
    insert_time_ns DateTime64(9) MATERIALIZED now64(9), 
    insert_replica_id UInt8 MATERIALIZED randConstant()
) ENGINE=ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'r1') 
ORDER BY id;

CREATE TABLE IF NOT EXISTS replicated_deduplicate_by_columns_r2 (
    id Int32, 
    value UInt32, 
    insert_time_ns DateTime64(9) MATERIALIZED now64(9), 
    insert_replica_id UInt8 MATERIALIZED randConstant()
) ENGINE=ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'r2') 
ORDER BY id;
```

For the `ReplicatedMergeTree` engine we give the path to the table and name of the replica in Zookeeper.

Insert some data into both replicas and wait for them to sync:

```sql
SYSTEM SYNC REPLICA replicated_deduplicate_by_columns_r2;
SYSTEM SYNC REPLICA replicated_deduplicate_by_columns_r1;
```

Check that we have data on replicas:

```sql
SELECT 'r1', id, value, count(), uniqExact(insert_time_ns), uniqExact(insert_replica_id) 
FROM replicated_deduplicate_by_columns_r1 
GROUP BY id, value 
ORDER BY id, value;

SELECT 'r2', id, value, count(), uniqExact(insert_time_ns), uniqExact(insert_replica_id) 
FROM replicated_deduplicate_by_columns_r2 
GROUP BY id, value 
ORDER BY id, value;
```

Remove full duplicates from replica `r1` based on all columns:

```sql
OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE;
```

Remove duplicates from replica `r1` based on all columns that are not `ALIAS` or `MATERIALIZED`:

```sql
OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE BY *; -- except insert_time_ns, insert_replica_id
```

Deduplicate replica `r1` explicitly by `id` and `value` columns:

```sql
OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE BY id, value;
```

Deduplicate by columns matching a regex:

```sql
OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE BY COLUMNS('[id, value]');

OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE BY COLUMNS('[i]') EXCEPT(insert_time_ns, insert_replica_id);
```

Don't forget to `DROP` tables and replicas `SYSTEM DROP REPLICA` afterwards.
