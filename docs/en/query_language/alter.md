## ALTER {#query_language_queries_alter}

The `ALTER` query is only supported for `*MergeTree` tables, as well as `Merge`and`Distributed`. The query has several variations.

### Column Manipulations

Changing the table structure.

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] ADD|DROP|CLEAR|MODIFY COLUMN ...
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
CLEAR COLUMN column_name IN PARTITION partition_name
```

Clears data in a column in a specified partition.

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

It only works for tables in the [MergeTree](../operations/table_engines/mergetree.md) family (including
[replicated](../operations/table_engines/replication.md) tables). The command changes the
[sorting key](../operations/table_engines/mergetree.md) of the table
to `new_expression` (an expression or a tuple of expressions). Primary key remains the same.

The command is lightweight in a sense that it only changes metadata. To keep the property that data part
rows are ordered by the sorting key expression you cannot add expressions containing existing columns
to the sorting key (only columns added by the `ADD COLUMN` command in the same `ALTER` query).


### Manipulations With Partitions and Parts {#alter_manipulations-with-partitions}

A partition in a table is a logic combination of records by a certain criterion. For example, you can combine data in a partition by month, day or hour. Each partition is stored separately in order to simplify manipulations with this data.

You can set the partition in the clause `PARTITION BY` when creating a table. For detailed information, see the section [Custom Partitioning Key](../operations/table_engines/custom_partitioning_key.md).

A "part" in a table is a part of data in a single partition, sorted by primary key. When inserting records into a partition, a new record firstly is stored as separate part (chunk) of a partition, until it is merged with other records into one part.

Partitions of a table are placed in the directory of this table: `/var/lib/clickhouse/data/<database>/<table>/`. For example: 

```bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 28
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 11:06 201901_1_1_0 
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 11:06 201901_2_2_0 
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 11:06 201901_3_3_0 
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 11:06 201902_4_4_0 
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 11:06 201902_5_5_0 
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 11:06 detached
```

In this example, the `visits` table has partitioning for a single month. The folders (`201901_1_1_0`, `201901_2_2_0` and so on) are the directories of parts; '201901' and '201902' are the names of the partitions. Each part relates to a correspond partition and contains data just for one month. When accessing the data for a specified month, ClickHouse will use just the correspond partition.

The `detached` directory contains parts that are not used by the server - detached from the table using the [DETACH](#alter_detach-partition) query. Parts that are damaged are also moved to this directory, instead of deleting them. You can add, delete, or modify the data in the `detached` directory at any time – the server will not know about this until you make the [ATTACH](#alter_attach-partition) query.

Note that there are several directories for the single partition in the table directory. This means that the parts of the partition are not merged yet. Clickhouse merges the inserted parts of data periodically, approximately 15 minutes after inserting. You can perform merge of parts using the [OPTIMIZE](misc.md#misc_operations-optimize) query. Example:

``` sql
OPTIMIZE TABLE visits PARTITION 201901;
OPTIMIZE TABLE visits PARTITION 201902;
```

Now the data table directory contains just two parts:

``` bash
$:/var/lib/clickhouse/data/default/visits#
total 28
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 09:49 201901_1_4_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 10:08 201902_5_7_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Jan 25 09:48 detached
```

Let's break down the name of the first part: `201901_1_1_0`.

- `201901` is the partition id that contains data for January.
- `1` is the minimum number of the data block.
- `1` is the maximum number of the data block.
- `0` is the chunk level (the depth of the merge tree it is formed from).

Note: for old-type tables part directories have name: `20140317_20140323_2_2_0` (minimum date - maximum date - minimum block number - maximum block number - level).


Use the `system.parts` table to view the set of table parts and partitions. The example is represented below.

``` sql
SELECT 
    partition,
    name, 
    active
FROM system.parts 
WHERE table = 'visits'
```

```
┌─partition───┬─directory──────┬─active─┐
│ 201901      │ 201901_1_1_0   │      0 │
│ 201901      │ 201901_1_3_1   │      1 │
│ 201901      │ 201901_2_2_0   │      0 │
│ 201901      │ 201901_3_3_0   │      0 │
│ 201902      │ 201902_4_4_0   │      0 │
│ 201902      │ 201902_4_5_1   │      1 │
│ 201902      │ 201902_5_5_0   │      0 │
└─────────────┴────────────────┴────────┘
```

The parts that have a field `active` with the value `0`, are inactive. The inactive parts are, for example, source parts remaining after merging to a larger part – these parts are deleted approximately 10 minutes after merging.

On an operating server, you can not manually change the set of parts or their data on the file system, since the server will not know about it.
For non-replicated tables, you can do this when the server is stopped, but we don't recommended it.
For replicated tables, the set of parts can't be changed in any case.

The following operations are available:

- [DETACH PARTITION](#alter_detach-partition) – Moves a partition to the 'detached' directory and forget it.
- [DROP PARTITION](#alter_drop-partition) – Deletes a partition.
- [ATTACH PART|PARTITION](#alter_attach-partition) – Adds a new part or partition from the `detached` directory to the table.
- [REPLACE PARTITION](#alter_replace-partition) - Copies the data of specified partition from another table.
- [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) - Resets the value of a specified column in a partition.
- [FREEZE PARTITION](#alter_freeze-partition) – Creates a backup of a partition.
- [FETCH PARTITION](#alter_fetch-partition) – Downloads a partition from another server.

Before reading the query descriptions, please, read the general rules of using partitions names in queries:

**Rules of specifying the partition name in queries**

- To specify a partition in ALTER PARTITION queries, specify the value of the partition expression. Constants and constant expressions are supported. Example: `ALTER TABLE table DROP PARTITION (toMonday(today()), 1)` (deletes the partition for the current week with event type 1). 
- For old-style tables, the partition can be specified either as a number `201901` or a string `'201901'`. The syntax for the new style of tables is stricter with types (similar to the parser for the VALUES input format). In addition, ALTER TABLE FREEZE PARTITION uses exact match for new-style tables (not prefix match).
- In the `system.parts` table, the `partition` column specifies the value of the partition expression to use in ALTER queries (if quotas are removed). The `name` column should specify the name of the data part that has a new format.
- To specify the only partition in a non-partitioned table, specify `PARTITION tuple()`.
- The partition ID is its string identifier (human-readable, if possible) that is used for the names of data parts in the file system and in ZooKeeper. You can specify it in ALTER queries in place of the partition key. Example: Partition key `toYYYYMM(EventDate)`; ALTER can specify either `PARTITION 201710` or `PARTITION ID '201710'`.

The same is true for the OPTIMIZE query. 

For examples, see the tests [`00502_custom_partitioning_local`](https://github.com/yandex/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00502_custom_partitioning_local.sql) and [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/yandex/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

#### DETACH PARTITION {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION name
```

Move all data for partitions named 'name' to the `detached` directory and forget about them.
It can be indicated in single quotes or without them.

After the query is executed, you can do whatever you want with the data in the 'detached' directory — delete it from the file system, or just leave it.

The query is replicated – data will be moved to the 'detached' directory and forgotten on all replicas. The query can only be sent to a leader replica. To find out if a replica is a leader, perform SELECT to the 'system.replicas' system table. Alternatively, it is easier to make a query on all replicas, and all except one will throw an exception.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION name
```

The same as the `DETACH` operation. Deletes data from the table. Data parts will be tagged as inactive and will be completely deleted in approximately 10 minutes. The query is replicated – data will be deleted on all replicas.

#### ATTACH PARTITION|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART name
```

Adds data to the table from the 'detached' directory.

It is possible to add data for an entire partition or a separate part. For a part, specify the full name of the part in single quotes.

The query is replicated. Each replica checks whether there is data in the 'detached' directory. If there is data, it checks the integrity, verifies that it matches the data on the server that initiated the query, and then adds it if everything is correct. If not, it downloads data from the query requestor replica, or from another replica where the data has already been added.

So you can put data in the 'detached' directory on one replica, and use the ALTER ... ATTACH query to add it to the table on all replicas.

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2_name REPLACE PARTITION name FROM table1_name
```

Copies data of partition from the `table1` to `table2`. Note that:

- Both tables must have the same structure.
- When creating the `table2`, the same partition format key must be determined for this table as for the `table1`. 

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_name
```

Resets the column value of a specified partition. If the `DEFAULT` clause was determined when creating a table, this query will set the column value to a specified default value.

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION name]
```

Creates a local backup of one or multiple partitions. The name can be the full name of the partition (for example, 201403), or its prefix (for example, 2014): then the backup will be created for all the corresponding partitions. If the partition name is omitted, the backup of all partitions will be created at once.

The query does the following: for a data snapshot at the time of execution, it creates hardlinks to table data in the directory `/var/lib/clickhouse/shadow/N/...`

`/var/lib/clickhouse/` is the working ClickHouse directory from the config. `N` is the incremental number of the backup.

The same structure of directories is created inside the backup as inside `/var/lib/clickhouse/`. It also performs 'chmod' for all files, forbidding writes to them.

The backup is created almost instantly (but first it waits for current queries to the corresponding table to finish running). At first, the backup doesn't take any space on the disk. As the system works, the backup can take disk space, as data is modified. If the backup is made for old enough data, it won't take space on the disk.

After creating the backup, data from `/var/lib/clickhouse/shadow/` can be copied to the remote server and then deleted on the local server. The entire backup process is performed without stopping the server.

The `ALTER ... FREEZE PARTITION` query is not replicated. A local backup is only created on the local server.

As an alternative, you can manually copy data from the `/var/lib/clickhouse/data/database/table` directory. But if you do this while the server is running, race conditions are possible when copying directories with files being added or changed, and the backup may be inconsistent. You can do this if the server isn't running – then the resulting data will be the same as after the `ALTER TABLE t FREEZE PARTITION` query.

`ALTER TABLE ... FREEZE PARTITION` only copies data, not table metadata. To make a backup of table metadata, copy the file `/var/lib/clickhouse/metadata/database/table.sql`

To restore from a backup:

- Use the CREATE query to create the table if it doesn't exist. The query can be taken from an .sql file (replace `ATTACH` in it with `CREATE`).
- Copy the data from the data/database/table/ directory inside the backup to the `/var/lib/clickhouse/data/database/table/detached/ directory.`
- Run `ALTER TABLE ... ATTACH PARTITION YYYYMM` queries, where `YYYYMM` is the month, for every month.

In this way, data from the backup will be added to the table.
Restoring from a backup doesn't require stopping the server.

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION name FROM 'path-in-zookeeper'
```

This query only works for replicatable tables.

It downloads the specified partition from the shard that has its `ZooKeeper path` specified in the `FROM` clause, then puts it in the `detached` directory for the specified table.

Although the query is called `ALTER TABLE`, it does not change the table structure, and does not immediately change the data available in the table.

Data is placed in the `detached` directory. You can use the `ALTER TABLE ... ATTACH` query to attach the data.

The `FROM` clause specifies the path in `ZooKeeper`. For example, `/clickhouse/tables/01-01/visits`.
Before downloading, the system checks that the partition exists and the table structure matches. The most appropriate replica is selected automatically from the healthy replicas.

The `ALTER ... FETCH PARTITION` query is not replicated. The partition will be downloaded to the 'detached' directory only on the local server. Note that if after this you use the `ALTER TABLE ... ATTACH` query to add data to the table, the data will be added on all replicas (on one of the replicas it will be added from the 'detached' directory, and on the rest it will be loaded from neighboring replicas).

### Synchronicity of ALTER Queries

For non-replicatable tables, all `ALTER` queries are performed synchronously. For replicatable tables, the query just adds instructions for the appropriate actions to `ZooKeeper`, and the actions themselves are performed as soon as possible. However, the query can wait for these actions to be completed on all the replicas.

For `ALTER ... ATTACH|DETACH|DROP` queries, you can use the `replication_alter_partitions_sync` setting to set up waiting.
Possible values: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### Mutations {#query_language_queries_show_databases}

Mutations are an ALTER query variant that allows changing or deleting rows in a table. In contrast to standard `UPDATE` and `DELETE` queries that are intended for point data changes, mutations are intended for heavy operations that change a lot of rows in a table.

The functionality is in beta stage and is available starting with the 1.1.54388 version. Currently *MergeTree table engines are supported (both replicated and unreplicated).

Existing tables are ready for mutations as-is (no conversion necessary), but after the first mutation is applied to a table, its metadata format becomes incompatible with previous server versions and falling back to a previous version becomes impossible.

Currently available commands:

``` sql
ALTER TABLE table_name DELETE WHERE filter_expr
```

The `filter_expr` must be of type UInt8. The query deletes rows in the table for which this expression takes a non-zero value.

``` sql
ALTER TABLE table_name UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

The command is available starting with the 18.12.14 version. The `filter_expr` must be of type UInt8. This query updates values of specified columns to the values of corresponding expressions in rows for which the `filter_expr` takes a non-zero value. Values are casted to the column type using the `CAST` operator. Updating columns that are used in the calculation of the primary or the partition key is not supported.

One query can contain several commands separated by commas.

For *MergeTree tables mutations execute by rewriting whole data parts. There is no atomicity - parts are substituted for mutated parts as soon as they are ready and a `SELECT` query that started executing during a mutation will see data from parts that have already been mutated along with data from parts that have not been mutated yet.

Mutations are totally ordered by their creation order and are applied to each part in that order. Mutations are also partially ordered with INSERTs - data that was inserted into the table before the mutation was submitted will be mutated and data that was inserted after that will not be mutated. Note that mutations do not block INSERTs in any way.

A mutation query returns immediately after the mutation entry is added (in case of replicated tables to ZooKeeper, for nonreplicated tables - to the filesystem). The mutation itself executes asynchronously using the system profile settings. To track the progress of mutations you can use the `system.mutations` table. A mutation that was successfully submitted will continue to execute even if ClickHouse servers are restarted. There is no way to roll back the mutation once it is submitted.

Entries for finished mutations are not deleted right away (the number of preserved entries is determined by the `finished_mutations_to_keep` storage engine parameter). Older mutation entries are deleted.

#### system.mutations Table

The table contains information about mutations of MergeTree tables and their progress. Each mutation command is represented by a single row. The table has the following columns:

**database**, **table** - The name of the database and table to which the mutation was applied.

**mutation_id** - The ID of the mutation. For replicated tables these IDs correspond to znode names in the `<table_path_in_zookeeper>/mutations/` directory in ZooKeeper. For unreplicated tables the IDs correspond to file names in the data directory of the table.

**command** - The mutation command string (the part of the query after `ALTER TABLE [db.]table`).

**create_time** - When this mutation command was submitted for execution.

**block_numbers.partition_id**, **block_numbers.number** - A Nested column. For mutations of replicated tables contains one record for each partition: the partition ID and the block number that was acquired by the mutation (in each partition only parts that contain blocks with numbers less than the block number acquired by the mutation in that partition will be mutated). Because in non-replicated tables blocks numbers in all partitions form a single sequence, for mutatations of non-replicated tables the column will contain one record with a single block number acquired by the mutation.

**parts_to_do** - The number of data parts that need to be mutated for the mutation to finish.

**is_done** - Is the mutation done? Note that even if `parts_to_do = 0` it is possible that a mutation of a replicated table is not done yet because of a long-running INSERT that will create a new data part that will need to be mutated.


[Original article](https://clickhouse.yandex/docs/en/query_language/alter/) <!--hide-->
