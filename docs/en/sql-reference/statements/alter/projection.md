---
sidebar_position: 49
sidebar_label: PROJECTION
---

# Manipulating Projections

The following operations with [projections](../../../engines/table-engines/mergetree-family/mergetree.md#projections) are available:

-   `ALTER TABLE [db].name ADD PROJECTION name ( SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY] )` - Adds projection description to tables metadata.

-   `ALTER TABLE [db].name DROP PROJECTION name` - Removes projection description from tables metadata and deletes projection files from disk. Implemented as a [mutation](../../../sql-reference/statements/alter/index.md#mutations).

-   `ALTER TABLE [db.]table MATERIALIZE PROJECTION name IN PARTITION partition_name` - The query rebuilds the projection `name` in the partition `partition_name`. Implemented as a [mutation](../../../sql-reference/statements/alter/index.md#mutations).

-   `ALTER TABLE [db.]table CLEAR PROJECTION name IN PARTITION partition_name` - Deletes projection files from disk without removing description. Implemented as a [mutation](../../../sql-reference/statements/alter/index.md#mutations).


The commands `ADD`, `DROP` and `CLEAR` are lightweight in a sense that they only change metadata or remove files.

Also, they are replicated, syncing projections metadata via ZooKeeper.

:::note    
Projection manipulation is supported only for tables with [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) engine (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) variants).
:::