---
slug: /en/sql-reference/statements/alter/statistics
sidebar_position: 45
sidebar_label: STATISTICS
---

# Manipulating Column Statistics

The following operations are available:

-   `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - Adds statistic description to tables metadata.

-   `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - Modifies statistic description to tables metadata.

-   `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - Removes statistics from the metadata of the specified columns and deletes all statistics objects in all parts for the specified columns.

-   `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - Deletes all statistics objects in all parts for the specified columns. Statistics objects can be rebuild using `ALTER TABLE MATERIALIZE STATISTICS`.

-   `ALTER TABLE [db.]table MATERIALIZE STATISTICS [IF EXISTS] (column list)` - Rebuilds the statistic for columns. Implemented as a [mutation](../../../sql-reference/statements/alter/index.md#mutations). 

The first two commands are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing statistics metadata via ZooKeeper.

## Example:

Adding two statistics types to two columns:

```
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
Statistic are supported only for [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) engine tables (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) variants).
:::
