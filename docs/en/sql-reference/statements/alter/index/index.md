---
toc_hidden_folder: true
sidebar_position: 42
sidebar_label: INDEX
---

# Manipulating Data Skipping Indices

The following operations are available:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value [FIRST|AFTER name]` - Adds index description to tables metadata.

-   `ALTER TABLE [db].name DROP INDEX name` - Removes index description from tables metadata and deletes index files from disk.

-   `ALTER TABLE [db.]table MATERIALIZE INDEX name [IN PARTITION partition_name]` - Rebuilds the secondary index `name` for the specified `partition_name`. Implemented as a [mutation](../../../../sql-reference/statements/alter/index.md#mutations). If `IN PARTITION` part is omitted then it rebuilds the index for the whole table data.

The first two commands are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing indices metadata via ZooKeeper.

:::note    
Index manipulation is supported only for tables with [`*MergeTree`](../../../../engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](../../../../engines/table-engines/mergetree-family/replication.md) variants).
:::
