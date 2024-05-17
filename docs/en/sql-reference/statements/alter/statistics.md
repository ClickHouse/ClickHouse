---
slug: /en/sql-reference/statements/alter/statistics
sidebar_position: 45
sidebar_label: STATISTICS
---

# Manipulating Column Statistics

The following operations are available:

-   `ALTER TABLE [db].table ADD STATISTICS (columns list) TYPE (type list)` - Adds statistic description to tables metadata.

-   `ALTER TABLE [db].table MODIFY STATISTICS (columns list) TYPE (type list)` - Modifies statistic description to tables metadata.

-   `ALTER TABLE [db].table DROP STATISTICS (columns list)` - Removes statistic description from tables metadata and deletes statistic files from disk.

-   `ALTER TABLE [db].table CLEAR STATISTICS (columns list)` - Deletes statistic files from disk.

-   `ALTER TABLE [db.]table MATERIALIZE STATISTICS (columns list)` - Rebuilds the statistic for columns. Implemented as a [mutation](../../../sql-reference/statements/alter/index.md#mutations). 

The first two commands are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing statistics metadata via ZooKeeper.

There is an example adding two statistics types to two columns:

```
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note    
Statistic manipulation is supported only for tables with [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) variants).
:::
