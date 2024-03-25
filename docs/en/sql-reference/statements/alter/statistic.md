---
slug: /en/sql-reference/statements/alter/statistic
sidebar_position: 45
sidebar_label: STATISTIC
---

# Manipulating Column Statistics

The following operations are available:

-   `ALTER TABLE [db].table ADD STATISTIC (columns list) TYPE type` - Adds statistic description to tables metadata.

-   `ALTER TABLE [db].table DROP STATISTIC (columns list) TYPE type` - Removes statistic description from tables metadata and deletes statistic files from disk.

-   `ALTER TABLE [db].table CLEAR STATISTIC (columns list) TYPE type` - Deletes statistic files from disk.

-   `ALTER TABLE [db.]table MATERIALIZE STATISTIC (columns list) TYPE type` - Rebuilds the statistic for columns. Implemented as a [mutation](../../../sql-reference/statements/alter/index.md#mutations). 

The first two commands are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing statistics metadata via ZooKeeper.

:::note    
Statistic manipulation is supported only for tables with [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) variants).
:::
