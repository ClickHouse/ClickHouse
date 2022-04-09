---
toc_hidden_folder: true
sidebar_position: 42
sidebar_label: INDEX
---

# Manipulating Data Skipping Indices {#manipulations-with-data-skipping-indices}

The following operations are available:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value [FIRST|AFTER name]` - Adds index description to tables metadata.

-   `ALTER TABLE [db].name DROP INDEX name` - Removes index description from tables metadata and deletes index files from disk.

-   `ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name` - The query rebuilds the secondary index `name` in the partition `partition_name`. Implemented as a [mutation](../../../../sql-reference/statements/alter/index.md#mutations). To rebuild index over the whole data in the table you need to remove `IN PARTITION` from query.

The first two commands are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing indices metadata via ZooKeeper.

:::note    
Index manipulation is supported only for tables with [`*MergeTree`](../../../../engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](../../../../engines/table-engines/mergetree-family/replication.md) variants).
:::