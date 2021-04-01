---
toc_hidden_folder: true
toc_priority: 42
toc_title: INDEX
---

# Manipulating Data Skipping Indices {#manipulations-with-data-skipping-indices}

The following operations are available:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` - Adds index description to tables metadata.

-   `ALTER TABLE [db].name DROP INDEX name` - Removes index description from tables metadata and deletes index files from disk.

-   `ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name` - The query rebuilds the secondary index `name` in the partition `partition_name`. Implemented as a [mutation](../../../../sql-reference/statements/alter/index.md#mutations).

The first two commands are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing indices metadata via ZooKeeper.

!!! note "Note"
    Index manipulation is supported only for tables with [`*MergeTree`](../../../../engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](../../../../engines/table-engines/mergetree-family/replication.md) variants).
