---
toc_priority: 38
toc_title: PARTITION
---

# Manipulating Partitions and Parts {#alter_manipulations-with-partitions}

The following operations with [partitions](../../../engines/table-engines/mergetree-family/custom-partitioning-key.md) are available:

-   [DETACH PARTITION](#alter_detach-partition) — Moves a partition to the `detached` directory and forget it.
-   [DROP PARTITION](#alter_drop-partition) — Deletes a partition.
-   [ATTACH PART\|PARTITION](#alter_attach-partition) — Adds a part or partition from the `detached` directory to the table.
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) — Copies the data partition from one table to another and adds.
-   [REPLACE PARTITION](#alter_replace-partition) — Copies the data partition from one table to another and replaces.
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition) — Moves the data partition from one table to another.
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) — Resets the value of a specified column in a partition.
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) — Resets the specified secondary index in a partition.
-   [FREEZE PARTITION](#alter_freeze-partition) — Creates a backup of a partition.
-   [FETCH PARTITION](#alter_fetch-partition) — Downloads a partition from another server.
-   [MOVE PARTITION\|PART](#alter_move-partition) — Move partition/data part to another disk or volume.

<!-- -->

## DETACH PARTITION {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

Moves all data for the specified partition to the `detached` directory. The server forgets about the detached data partition as if it does not exist. The server will not know about this data until you make the [ATTACH](#alter_attach-partition) query.

Example:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

Read about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

After the query is executed, you can do whatever you want with the data in the `detached` directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the `detached` directory on all replicas. Note that you can execute this query only on a leader replica. To find out if a replica is a leader, perform the `SELECT` query to the [system.replicas](../../../operations/system-tables/replicas.md#system_tables-replicas) table. Alternatively, it is easier to make a `DETACH` query on all replicas - all the replicas throw an exception, except the leader replica.

## DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

Deletes the specified partition from the table. This query tags the partition as inactive and deletes data completely, approximately in 10 minutes.

Read about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

The query is replicated – it deletes data on all replicas.

## DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

Removes the specified part or all parts of the specified partition from `detached`.
Read more about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

## ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Adds data to the table from the `detached` directory. It is possible to add data for an entire partition or for a separate part. Examples:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Read more about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

This query is replicated. The replica-initiator checks whether there is data in the `detached` directory. If data exists, the query checks its integrity. If everything is correct, the query adds the data to the table. All other replicas download the data from the replica-initiator.

So you can put data to the `detached` directory on one replica, and use the `ALTER ... ATTACH` query to add it to the table on all replicas.

## ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

This query copies the data partition from the `table1` to `table2` adds data to exsisting in the `table2`. Note that data won’t be deleted from `table1`.

For the query to run successfully, the following conditions must be met:

-   Both tables must have the same structure.
-   Both tables must have the same partition key.

## REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

This query copies the data partition from the `table1` to `table2` and replaces existing partition in the `table2`. Note that data won’t be deleted from `table1`.

For the query to run successfully, the following conditions must be met:

-   Both tables must have the same structure.
-   Both tables must have the same partition key.

## MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

This query moves the data partition from the `table_source` to `table_dest` with deleting the data from `table_source`.

For the query to run successfully, the following conditions must be met:

-   Both tables must have the same structure.
-   Both tables must have the same partition key.
-   Both tables must be the same engine family (replicated or non-replicated).
-   Both tables must have the same storage policy.

## CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

Resets all values in the specified column in a partition. If the `DEFAULT` clause was determined when creating a table, this query sets the column value to a specified default value.

Example:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

## FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

This query creates a local backup of a specified partition. If the `PARTITION` clause is omitted, the query creates the backup of all partitions at once.

!!! note "Note"
    The entire backup process is performed without stopping the server.

Note that for old-styled tables you can specify the prefix of the partition name (for example, ‘2019’) - then the query creates the backup for all the corresponding partitions. Read about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

At the time of execution, for a data snapshot, the query creates hardlinks to a table data. Hardlinks are placed in the directory `/var/lib/clickhouse/shadow/N/...`, where:

-   `/var/lib/clickhouse/` is the working ClickHouse directory specified in the config.
-   `N` is the incremental number of the backup.

!!! note "Note"
    If you use [a set of disks for data storage in a table](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes), the `shadow/N` directory appears on every disk, storing data parts that matched by the `PARTITION` expression.

The same structure of directories is created inside the backup as inside `/var/lib/clickhouse/`. The query performs ‘chmod’ for all files, forbidding writing into them.

After creating the backup, you can copy the data from `/var/lib/clickhouse/shadow/` to the remote server and then delete it from the local server. Note that the `ALTER t FREEZE PARTITION` query is not replicated. It creates a local backup only on the local server.

The query creates backup almost instantly (but first it waits for the current queries to the corresponding table to finish running).

`ALTER TABLE t FREEZE PARTITION` copies only the data, not table metadata. To make a backup of table metadata, copy the file `/var/lib/clickhouse/metadata/database/table.sql`

To restore data from a backup, do the following:

1.  Create the table if it does not exist. To view the query, use the .sql file (replace `ATTACH` in it with `CREATE`).
2.  Copy the data from the `data/database/table/` directory inside the backup to the `/var/lib/clickhouse/data/database/table/detached/` directory.
3.  Run `ALTER TABLE t ATTACH PARTITION` queries to add the data to a table.

Restoring from a backup doesn’t require stopping the server.

For more information about backups and restoring data, see the [Data Backup](../../../operations/backup.md) section.

## CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

The query works similar to `CLEAR COLUMN`, but it resets an index instead of a column data.

## FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

Downloads a partition from another server. This query only works for the replicated tables.

The query does the following:

1.  Downloads the partition from the specified shard. In ‘path-in-zookeeper’ you must specify a path to the shard in ZooKeeper.
2.  Then the query puts the downloaded data to the `detached` directory of the `table_name` table. Use the [ATTACH PARTITION\|PART](#alter_attach-partition) query to add the data to the table.

For example:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

Note that:

-   The `ALTER ... FETCH PARTITION` query isn’t replicated. It places the partition to the `detached` directory only on the local server.
-   The `ALTER TABLE ... ATTACH` query is replicated. It adds the data to all replicas. The data is added to one of the replicas from the `detached` directory, and to the others - from neighboring replicas.

Before downloading, the system checks if the partition exists and the table structure matches. The most appropriate replica is selected automatically from the healthy replicas.

Although the query is called `ALTER TABLE`, it does not change the table structure and does not immediately change the data available in the table.

## MOVE PARTITION\|PART {#alter_move-partition}

Moves partitions or data parts to another volume or disk for `MergeTree`-engine tables. See [Using Multiple Block Devices for Data Storage](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

The `ALTER TABLE t MOVE` query:

-   Not replicated, because different replicas can have different storage policies.
-   Returns an error if the specified disk or volume is not configured. Query also returns an error if conditions of data moving, that specified in the storage policy, can’t be applied.
-   Can return an error in the case, when data to be moved is already moved by a background process, concurrent `ALTER TABLE t MOVE` query or as a result of background data merging. A user shouldn’t perform any additional actions in this case.

Example:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

## How to Set Partition Expression {#alter-how-to-specify-part-expr}

You can specify the partition expression in `ALTER ... PARTITION` queries in different ways:

-   As a value from the `partition` column of the `system.parts` table. For example, `ALTER TABLE visits DETACH PARTITION 201901`.
-   As the expression from the table column. Constants and constant expressions are supported. For example, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   Using the partition ID. Partition ID is a string identifier of the partition (human-readable, if possible) that is used as the names of partitions in the file system and in ZooKeeper. The partition ID must be specified in the `PARTITION ID` clause, in a single quotes. For example, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   In the [ALTER ATTACH PART](#alter_attach-partition) and [DROP DETACHED PART](#alter_drop-detached) query, to specify the name of a part, use string literal with a value from the `name` column of the [system.detached\_parts](../../../operations/system-tables/detached_parts.md#system_tables-detached_parts) table. For example, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

Usage of quotes when specifying the partition depends on the type of partition expression. For example, for the `String` type, you have to specify its name in quotes (`'`). For the `Date` and `Int*` types no quotes are needed.

All the rules above are also true for the [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) query. If you need to specify the only partition when optimizing a non-partitioned table, set the expression `PARTITION tuple()`. For example:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

The examples of `ALTER ... PARTITION` queries are demonstrated in the tests [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) and [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).
