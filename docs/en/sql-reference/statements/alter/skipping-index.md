---
slug: /en/sql-reference/statements/alter/skipping-index

toc_hidden_folder: true
sidebar_position: 42
sidebar_label: INDEX
---

# Manipulating Data Skipping Indices

The following operations are available:

## ADD INDEX

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - Adds index description to tables metadata.

## DROP INDEX

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - Removes index description from tables metadata and deletes index files from disk. Implemented as a [mutation](/docs/en/sql-reference/statements/alter/index.md#mutations).

## MATERIALIZE INDEX

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Rebuilds the secondary index `name` for the specified `partition_name`. Implemented as a [mutation](/docs/en/sql-reference/statements/alter/index.md#mutations). If `IN PARTITION` part is omitted then it rebuilds the index for the whole table data.

## CLEAR INDEX

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Deletes the secondary index files from disk without removing description. Implemented as a [mutation](/docs/en/sql-reference/statements/alter/index.md#mutations).


The commands `ADD`, `DROP`, and `CLEAR` are lightweight in the sense that they only change metadata or remove files.
Also, they are replicated, syncing indices metadata via ClickHouse Keeper or ZooKeeper.

:::note    
Index manipulation is supported only for tables with [`*MergeTree`](/docs/en/engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](/docs/en/engines/table-engines/mergetree-family/replication.md) variants).
:::
