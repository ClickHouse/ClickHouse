---
toc_hidden_folder: true
sidebar_position: 42
sidebar_label: STATISTIC
---

# Manipulating Column Statistics

The following operations are available:

-   `ALTER TABLE [db].name ADD STATISTIC name (columns list) [TYPE type]` - Adds statistic description to tables metadata.

-   `ALTER TABLE [db].name DROP STATISTIC name` - Removes statistic description from tables metadata and deletes statistic files from disk.

-   `ALTER TABLE [db.]table MATERIALIZE STATISTIC name [IN PARTITION partition_name]` - Rebuilds the statistic `name` for the specified `partition_name`. Implemented as a [mutation](../../../../sql-reference/statements/alter/index.md#mutations). If `IN PARTITION` part is omitted then it rebuilds the statistic for the whole table data.

The first two commands are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing statistics metadata via ZooKeeper.

:::note    
Statistic manipulation is supported only for tables with [`*MergeTree`](../../../../engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](../../../../engines/table-engines/mergetree-family/replication.md) variants).
:::
