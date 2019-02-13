## ALTER {#query_language_queries_alter}

The `ALTER` query is only supported for `*MergeTree` tables, as well as `Merge`and`Distributed`. The query has several variations.

### Column Manipulations

Changing the table structure.

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|MODIFY COLUMN ...
```

In the query, specify a list of one or more comma-separated actions.
Each action is an operation on a column.

The following actions are supported:

``` sql
ADD COLUMN name [type] [default_expr] [AFTER name_after]
```

Adds a new column to the table with the specified name, type, and `default_expr` (see the section "Default expressions"). If you specify `AFTER name_after` (the name of another column), the column is added after the specified one in the list of table columns. Otherwise, the column is added to the end of the table. Note that there is no way to add a column to the beginning of a table. For a chain of actions, 'name_after' can be the name of a column that is added in one of the previous actions.

Adding a column just changes the table structure, without performing any actions with data. The data doesn't appear on the disk after ALTER. If the data is missing for a column when reading from the table, it is filled in with default values (by performing the default expression if there is one, or using zeros or empty strings). If the data is missing for a column when reading from the table, it is filled in with default values (by performing the default expression if there is one, or using zeros or empty strings). The column appears on the disk after merging data parts (see MergeTree).

This approach allows us to complete the ALTER query instantly, without increasing the volume of old data.

``` sql
DROP COLUMN name
```

Deletes the column with the name 'name'.

Deletes data from the file system. Since this deletes entire files, the query is completed almost instantly.

``` sql
CLEAR COLUMN name IN PARTITION partition_name
```

Clears all data in a column in a specified partition.

``` sql
MODIFY COLUMN name [type] [default_expr]
```

Changes the 'name' column's type to 'type' and/or the default expression to 'default_expr'. When changing the type, values are converted as if the 'toType' function were applied to them.

If only the default expression is changed, the query doesn't do anything complex, and is completed almost instantly.

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

There are several processing stages:

- Preparing temporary (new) files with modified data.
- Renaming old files.
- Renaming the temporary (new) files to the old names.
- Deleting the old files.

Only the first stage takes time. If there is a failure at this stage, the data is not changed.
If there is a failure during one of the successive stages, data can be restored manually. The exception is if the old files were deleted from the file system but the data for the new files did not get written to the disk and was lost.

The `ALTER` query lets you create and delete separate elements (columns) in nested data structures, but not whole nested data structures. To add a nested data structure, you can add columns with a name like `name.nested_name` and the type `Array(T)`. A nested data structure is equivalent to multiple array columns with a name that has the same prefix before the dot.

There is no support for deleting columns in the primary key or the sampling key (columns that are in the `ENGINE` expression). Changing the type for columns that are included in the primary key is only possible if this change does not cause the data to be modified (for example, it is allowed to add values to an Enum or change a type with `DateTime` to `UInt32`).

If the `ALTER` query is not sufficient for making the table changes you need, you can create a new table, copy the data to it using the `INSERT SELECT` query, then switch the tables using the `RENAME` query and delete the old table.

The `ALTER` query blocks all reads and writes for the table. In other words, if a long `SELECT` is running at the time of the `ALTER` query, the `ALTER` query will wait for it to complete. At the same time, all new queries to the same table will wait while this `ALTER` is running.

For tables that don't store data themselves (such as `Merge` and `Distributed`), `ALTER` just changes the table structure, and does not change the structure of subordinate tables. For example, when running ALTER for a `Distributed` table, you will also need to run `ALTER` for the tables on all remote servers.

The `ALTER` query for changing columns is replicated. The instructions are saved in ZooKeeper, then each replica applies them. All `ALTER` queries are run in the same order. The query waits for the appropriate actions to be completed on the other replicas. However, a query to change columns in a replicated table can be interrupted, and all actions will be performed asynchronously.


### Manipulations With Key Expressions

The following command is supported:

``` sql
MODIFY ORDER BY new_expression
```

It only works for tables in the [`MergeTree`](../operations/table_engines/mergetree.md) family (including
[replicated](../operations/table_engines/replication.md) tables). The command changes the
[sorting key](../operations/table_engines/mergetree.md) of the table
to `new_expression` (an expression or a tuple of expressions). Primary key remains the same.

The command is lightweight in a sense that it only changes metadata. To keep the property that data part
rows are ordered by the sorting key expression you cannot add expressions containing existing columns
to the sorting key (only columns added by the `ADD COLUMN` command in the same `ALTER` query).


### Manipulations With Data Skipping Indices

It only works for tables in the [`*MergeTree`](../operations/table_engines/mergetree.md) family (including
[replicated](../operations/table_engines/replication.md) tables). The following operations
are available:

* `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` - Adds index description to tables metadata.

* `ALTER TABLE [db].name DROP INDEX name` - Removes index description from tables metadata and deletes index files from disk.

These commands are lightweight in a sense that they only change metadata or remove files.
Also, they are replicated (syncing indices metadata through ZooKeeper).

### Manipulations With Partitions and Parts {#alter_manipulations-with-partitions}

The following operations with [partitions](../operations/table_engines/custom_partitioning_key.md) are available:

- [DETACH PARTITION](#alter_detach-partition) – Moves a partition to the 'detached' directory and forget it.
- [DROP PARTITION](#alter_drop-partition) – Deletes a partition.
- [ATTACH PART|PARTITION](#alter_attach-partition) – Adds a part or partition from the 'detached' directory to the table.
- [REPLACE PARTITION](#alter_replace-partition) - Copies the data partition from one table to another.
- [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) - Resets the value of a specified column in a partition.
- [FREEZE PARTITION](#alter_freeze-partition) – Creates a backup of a partition.
- [FETCH PARTITION](#alter_fetch-partition) – Downloads a partition from another server.

#### DETACH PARTITION {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

Moves all data for the specified partition to the 'detached' directory ([how to specify the partition expression](#alter-how-to-specify-part-expr)). The server forgets about the detached data partition as if it does not exist. The server will not know about this data until you make the [ATTACH](#alter_attach-partition) query.

Example:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

After the query is executed, you can do whatever you want with the data in the 'detached' directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the 'detached' directory on all replicas. Note that you can execute this query only on a leader replica. To find out if a replica is a leader, use the [system.replicas](../operations/system_tables.md#system_tables-replicas) table. Alternatively, it is easier to make a query on all replicas - all the replicas throw an exception, except the leader replica.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

Deletes the data of specified partition from the table ([how to specify the partition expression](#alter-how-to-specify-part-expr)). This query tags the partition as inactive and deletes data completely, approximately in 10 minutes.

The query is replicated – it deletes data on all replicas.

#### ATTACH PARTITION|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Adds data to the table from the 'detached' directory. It is possible to add data for an entire partition or for a separate part. Examples:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Read more about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

This query is replicated. Each replica checks whether there is data in the 'detached' directory. If the data is in this directory, the query checks the integrity, verifies that it matches the data on the server that initiated the query. If everything is correct, the query adds data to the replica. If not, it downloads data from the query requestor replica, or from another replica where the data has already been added.

So you can put data to the 'detached' directory on one replica, and use the `ALTER ... ATTACH` query to add it to the table on all replicas.

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2_name REPLACE PARTITION partition_expr FROM table1_name
```

This query copies the data partition from the `table1` to `table2`. Note that:

- Both tables must have the same structure.
- When creating the `table2`, you must specify the same partition key as for the `table1`. 

Read about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

Resets all values in the specified column in a partition. If the `DEFAULT` clause was determined when creating a table, this query sets the column value to a specified default value.

Example:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

This query creates a local backup of a specified partition. If the `PARTITION` clause is omitted, the query creates the backup of all partitions at once. Note that for old-styled tables you can specify the prefix of the partition name (for example, '2019') - then the query creates the backup for all the corresponding partitions.

At the time of execution, for a data snapshot, the query creates hardlinks to a table data. Hardlinks are placed in the directory `/var/lib/clickhouse/shadow/N/...`, where

- `/var/lib/clickhouse/` is the working ClickHouse directory specified in the config.
- `N` is the incremental number of the backup.

The same structure of directories is created inside the backup as inside `/var/lib/clickhouse/`. It also performs 'chmod' for all files, forbidding writing into them.

The query creates backup almost instantly (but first it waits for the current queries to the corresponding table to finish running). At first, the backup does not take any space on the disk. As the system works, the backup can take disk space, as data is modified. If the backup is made for old enough data, it does not take space on the disk.

After creating the backup, you can copy the data from `/var/lib/clickhouse/shadow/` to the remote server and then delete it from the local server. The entire backup process is performed without stopping the server.

The `ALTER ... FREEZE PARTITION` query is not replicated. It creates a local backup only on the local server.

As an alternative, you can manually copy data from the `/var/lib/clickhouse/data/database/table` directory. But if you do this while the server is running, race conditions are possible when copying directories with files being added or changed, and the backup may be inconsistent. Copy the data when the server is not running – then the resulting data will be the same as after the `ALTER TABLE t FREEZE PARTITION` query.

`ALTER TABLE ... FREEZE PARTITION` copies only the data, not table metadata. To make a backup of table metadata, copy the file `/var/lib/clickhouse/metadata/database/table.sql`

To add the data to a table from a backup, do the following:

1. Use the `CREATE` query to create the table if it does not exist. To view the query, use the .sql file (replace `ATTACH` in it with `CREATE`).
2. Copy the data from the `data/database/table/` directory inside the backup to the `/var/lib/clickhouse/data/database/table/detached/` directory.
3. Run `ALTER TABLE ... ATTACH PARTITION` queries to add the data to a table.

Restoring from a backup does not require stopping the server.

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

Downloads a partition from another server. This query only works for the replicated tables.

The query does the following:

1. Downloads the partition from the specified shard. In 'path-in-zookeeper' you must specify a path to the shard in ZooKeeper.
2. Then the query puts the downloaded data to the 'detached' directory of the specified table. Use the [ATTACH PART|PARTITION](#alter_attach-partition) query to add the data to the table.

For example:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

Before downloading, the system checks if the partition exists and the table structure matches. The most appropriate replica is selected automatically from the healthy replicas.

Although the query is called `ALTER TABLE`, it does not change the table structure and does not immediately change the data available in the table.

The `ALTER ... FETCH PARTITION` query is not replicated. It places the partition to the 'detached' directory only on the local server. Note that when you perform the `ALTER TABLE ... ATTACH` query, it adds the data to all replicas. The data is added to one of the replicas from the 'detached' directory, and to the others - from neighboring replicas.

#### How To Set Partition Expression {#alter-how-to-specify-part-expr}

You can specify the partition expression in `ALTER ... PARTITION` queries in different ways:

- As a value from the `partition` column of the `system.parts` table. For example, `ALTER TABLE visits DETACH PARTITION 201901`.
- As the expression from the table column. Constants and constant expressions are supported. For example, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
- Using the partition ID. Partition ID is a string identifier of the partition (human-readable, if possible) that is used as the names of partitions in the file system and in ZooKeeper. The partition ID must be specified in the `PARTITION ID` clause, in a single quotes. For example, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
- In the [ALTER ATTACH PART](#alter_attach-partition) query, to specify the name of a part, use a value from the `name` column of the `system.parts` table. For example, `ALTER TABLE visits ATTACH PART 201901_1_1_0`.

Correct usage of quotes in the partition expression depends on the type of the partition key, that was specified when creating a table. For example, for the String type partitions, you have to specify its name in quotes (`'`). For the Date and Int* types no quotes needed.

For old-style tables, you can specify the partition either as a number `201901` or a string `'201901'`. The syntax for the new-style tables is stricter with types (similar to the parser for the VALUES input format).

All the rules above are also true for the [OPTIMIZE](misc.md#misc_operations-optimize) query. If you need to specify the only partition when optimizing a non-partitioned table, set the expression `PARTITION tuple()`. For example:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

The examples of `ALTER ... PARTITION` queries are demonstrated in the tests [`00502_custom_partitioning_local`](https://github.com/yandex/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00502_custom_partitioning_local.sql) and [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/yandex/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### Synchronicity of ALTER Queries

For non-replicatable tables, all `ALTER` queries are performed synchronously. For replicatable tables, the query just adds instructions for the appropriate actions to `ZooKeeper`, and the actions themselves are performed as soon as possible. However, the query can wait for these actions to be completed on all the replicas.

For `ALTER ... ATTACH|DETACH|DROP` queries, you can use the `replication_alter_partitions_sync` setting to set up waiting.
Possible values: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### Mutations {#alter-mutations}

Mutations are an ALTER query variant that allows changing or deleting rows in a table. In contrast to standard `UPDATE` and `DELETE` queries that are intended for point data changes, mutations are intended for heavy operations that change a lot of rows in a table.

The functionality is in beta stage and is available starting with the 1.1.54388 version. Currently `*MergeTree` table engines are supported (both replicated and unreplicated).

Existing tables are ready for mutations as-is (no conversion necessary), but after the first mutation is applied to a table, its metadata format becomes incompatible with previous server versions and falling back to a previous version becomes impossible.

Currently available commands:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

The `filter_expr` must be of type UInt8. The query deletes rows in the table for which this expression takes a non-zero value.

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

The command is available starting with the 18.12.14 version. The `filter_expr` must be of type UInt8. This query updates values of specified columns to the values of corresponding expressions in rows for which the `filter_expr` takes a non-zero value. Values are casted to the column type using the `CAST` operator. Updating columns that are used in the calculation of the primary or the partition key is not supported.

One query can contain several commands separated by commas.

For *MergeTree tables mutations execute by rewriting whole data parts. There is no atomicity - parts are substituted for mutated parts as soon as they are ready and a `SELECT` query that started executing during a mutation will see data from parts that have already been mutated along with data from parts that have not been mutated yet.

Mutations are totally ordered by their creation order and are applied to each part in that order. Mutations are also partially ordered with INSERTs - data that was inserted into the table before the mutation was submitted will be mutated and data that was inserted after that will not be mutated. Note that mutations do not block INSERTs in any way.

A mutation query returns immediately after the mutation entry is added (in case of replicated tables to ZooKeeper, for nonreplicated tables - to the filesystem). The mutation itself executes asynchronously using the system profile settings. To track the progress of mutations you can use the [`system.mutations`](../operations/system_tables.md#system_tables-mutations) table. A mutation that was successfully submitted will continue to execute even if ClickHouse servers are restarted. There is no way to roll back the mutation once it is submitted, but if the mutation is stuck for some reason it can be cancelled with the [`KILL MUTATION`](misc.md#kill-mutation) query.

Entries for finished mutations are not deleted right away (the number of preserved entries is determined by the `finished_mutations_to_keep` storage engine parameter). Older mutation entries are deleted.

[Original article](https://clickhouse.yandex/docs/en/query_language/alter/) <!--hide-->
