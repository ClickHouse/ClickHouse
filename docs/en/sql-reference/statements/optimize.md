---
toc_priority: 49
toc_title: OPTIMIZE
---

# OPTIMIZE Statement {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

This query tries to initialize an unscheduled merge of data parts for tables with a table engine from the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family.

The `OPTMIZE` query is also supported for the [MaterializedView](../../engines/table-engines/special/materializedview.md) and the [Buffer](../../engines/table-engines/special/buffer.md) engines. Other table engines aren’t supported.

When `OPTIMIZE` is used with the [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) family of table engines, ClickHouse creates a task for merging and waits for execution on all nodes (if the `replication_alter_partitions_sync` setting is enabled).

-   If `OPTIMIZE` doesn’t perform a merge for any reason, it doesn’t notify the client. To enable notifications, use the [optimize\_throw\_if\_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) setting.
-   If you specify a `PARTITION`, only the specified partition is optimized. [How to set partition expression](../../sql-reference/statements/alter/index.md#alter-how-to-specify-part-expr).
-   If you specify `FINAL`, optimization is performed even when all the data is already in one part.
-   If you specify `DEDUPLICATE`, then completely identical rows will be deduplicated (all columns are compared), it makes sense only for the MergeTree engine.

!!! warning "Warning"
    `OPTIMIZE` can’t fix the “Too many parts” error.
