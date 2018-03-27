# Queries

## CREATE DATABASE

Creating db_name databases

```sql
CREATE DATABASE [IF NOT EXISTS] db_name
```

`A database` is just a directory for tables.
If `IF NOT EXISTS` is included, the query won't return an error if the database already exists.


<a name="query_language-queries-create_table"></a>

## CREATE TABLE

The `CREATE TABLE` query can have several forms.

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = engine
```

Creates a table named 'name' in the 'db' database or the current database if 'db' is not set, with the structure specified in brackets and the 'engine' engine.
The structure of the table is a list of column descriptions. If indexes are supported by the engine, they are indicated as parameters for the table engine.

A column description is `name type` in the simplest case. Example: `RegionID UInt32`.
Expressions can also be defined for default values (see below).

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name AS [db2.]name2 [ENGINE = engine]
```

Creates a table with the same structure as another table. You can specify a different engine for the table. If the engine is not specified, the same engine will be used as for the `db2.name2` table.

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name ENGINE = engine AS SELECT ...
```

Creates a table with a structure like the result of the `SELECT` query, with the 'engine' engine, and fills it with data from SELECT.

In all cases, if `IF NOT EXISTS` is specified, the query won't return an error if the table already exists. In this case, the query won't do anything.

### Default values

The column description can specify an expression for a default value, in one of the following ways:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
Example: `URLDomain String DEFAULT domain(URL)`.

If an expression for the default value is not defined, the default values will be set to zeros for numbers, empty strings for strings, empty arrays for arrays, and `0000-00-00` for dates or `0000-00-00 00:00:00` for dates with time. NULLs are not supported.

If the default expression is defined, the column type is optional. If there isn't an explicitly defined type, the default expression type is used. Example: `EventDate DEFAULT toDate(EventTime)` – the 'Date' type will be used for the 'EventDate' column.

If the data type and default expression are defined explicitly, this expression will be cast to the specified type using type casting functions. Example: `Hits UInt32 DEFAULT 0` means the same thing as `Hits UInt32 DEFAULT toUInt32(0)`.

Default expressions may be defined as an arbitrary expression from table constants and columns. When creating and changing the table structure, it checks that expressions don't contain loops. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

`DEFAULT expr`

Normal default value. If the INSERT query doesn't specify the corresponding column, it will be filled in by computing the corresponding expression.

`MATERIALIZED expr`

Materialized expression. Such a column can't be specified for INSERT, because it is always calculated.
For an INSERT without a list of columns, these columns are not considered.
In addition, this column is not substituted when using an asterisk in a SELECT query. This is to preserve the invariant that the dump obtained using `SELECT *` can be inserted back into the table using INSERT without specifying the list of columns.

`ALIAS expr`

Synonym. Such a column isn't stored in the table at all.
Its values can't be inserted in a table, and it is not substituted when using an asterisk in a SELECT query.
It can be used in SELECTs if the alias is expanded during query parsing.

When using the ALTER query to add new columns, old data for these columns is not written. Instead, when reading old data that does not have values for the new columns, expressions are computed on the fly by default. However, if running the expressions requires different columns that are not indicated in the query, these columns will additionally be read, but only for the blocks of data that need it.

If you add a new column to a table but later change its default expression, the values used for old data will change (for data where values were not stored on the disk). Note that when running background merges, data for columns that are missing in one of the merging parts is written to the merged part.

It is not possible to set default values for elements in nested data structures.

### Temporary tables

In all cases, if `TEMPORARY` is specified, a temporary table will be created. Temporary tables have the following characteristics:

- Temporary tables disappear when the session ends, including if the connection is lost.
- A temporary table is created with the Memory engine. The other table engines are not supported.
- The DB can't be specified for a temporary table. It is created outside of databases.
- If a temporary table has the same name as another one and a query specifies the table name without specifying the DB, the temporary table will be used.
- For distributed query processing, temporary tables used in a query are passed to remote servers.

In most cases, temporary tables are not created manually, but when using external data for a query, or for distributed `(GLOBAL) IN`. For more information, see the appropriate sections

Distributed DDL queries (ON CLUSTER clause)
----------------------------------------------

The `CREATE`, `DROP`, `ALTER`, and `RENAME` queries support distributed execution on a cluster.
For example, the following query creates the `all_hits` `Distributed` table on each host in `cluster`:

```sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

In order to run these queries correctly, each host must have the same cluster definition (to simplify syncing configs, you can use substitutions from ZooKeeper). They must also connect to the ZooKeeper servers.
The local version of the query will eventually be implemented on each host in the cluster, even if some hosts are currently not available. The order for executing queries within a single host is guaranteed.
` ALTER` queries are not yet supported for replicated tables.

## CREATE VIEW

```sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Creates a view. There are two types of views: normal and MATERIALIZED.

When creating a materialized view, you must specify ENGINE – the table engine for storing data.

A materialized view works as follows: when inserting data to the table specified in SELECT, part of the inserted data is converted by this SELECT query, and the result is inserted in the view.

Normal views don't store any data, but just perform a read from another table. In other words, a normal view is nothing more than a saved query. When reading from a view, this saved query is used as a subquery in the FROM clause.

As an example, assume you've created a view:

```sql
CREATE VIEW view AS SELECT ...
```

and written a query:

```sql
SELECT a, b, c FROM view
```

This query is fully equivalent to using the subquery:

```sql
SELECT a, b, c FROM (SELECT ...)
```

Materialized views store data transformed by the corresponding SELECT query.

When creating a materialized view, you must specify ENGINE – the table engine for storing data.

A materialized view is arranged as follows: when inserting data to the table specified in SELECT, part of the inserted data is converted by this SELECT query, and the result is inserted in the view.

If you specify POPULATE, the existing table data is inserted in the view when creating it, as if making a `CREATE TABLE ... AS SELECT ...` . Otherwise, the query contains only the data inserted in the table after creating the view. We don't recommend using POPULATE, since data inserted in the table during the view creation will not be inserted in it.

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`... Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won't be further aggregated. The exception is when using an ENGINE that independently performs data aggregation, such as `SummingMergeTree`.

The execution of `ALTER` queries on materialized views has not been fully developed, so they might be inconvenient. If the materialized view uses the construction ``TO [db.]name``, you can ``DETACH`` the view, run ``ALTER`` for the target table, and then ``ATTACH`` the previously detached (``DETACH``) view.

Views look the same as normal tables. For example, they are listed in the result of the `SHOW TABLES` query.

There isn't a separate query for deleting views. To delete a view, use `DROP TABLE`.

## ATTACH

This query is exactly the same as `CREATE`, but

- instead of the word `CREATE` it uses the word `ATTACH`.
- The query doesn't create data on the disk, but assumes that data is already in the appropriate places, and just adds information about the table to the server.
After executing an ATTACH query, the server will know about the existence of the table.

If the table was previously detached (``DETACH``), meaning that its structure is known, you can use shorthand without defining the structure.

```sql
ATTACH TABLE [IF NOT EXISTS] [db.]name
```

This query is used when starting the server. The server stores table metadata as files with `ATTACH` queries, which it simply runs at launch (with the exception of system tables, which are explicitly created on the server).

## DROP

This query has two types: `DROP DATABASE`  and `DROP TABLE`.

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

Deletes all tables inside the 'db' database, then deletes the 'db' database itself.
If `IF EXISTS` is specified, it doesn't return an error if the database doesn't exist.

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Deletes the table.
If `IF EXISTS` is specified, it doesn't return an error if the table doesn't exist or the database doesn't exist.

## DETACH

Deletes information about the 'name' table from the server. The server stops knowing about the table's existence.

```sql
DETACH TABLE [IF EXISTS] [db.]name
```

This does not delete the table's data or metadata. On the next server launch, the server will read the metadata and find out about the table again.
Similarly, a "detached" table can be re-attached using the `ATTACH` query (with the exception of system tables, which do not have metadata stored for them).

There is no `DETACH DATABASE` query.

## RENAME

Renames one or more tables.

```sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

All tables are renamed under global locking. Renaming tables is a light operation. If you indicated another database after TO, the table will be moved to this database. However, the directories with databases must reside in the same file system (otherwise, an error is returned).

<a name="query_language_queries_alter"></a>

## ALTER

The `ALTER` query is only supported for `*MergeTree` tables, as well as `Merge`and`Distributed`. The query has several variations.

### Column manipulations

Changing the table structure.

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|MODIFY COLUMN ...
```

In the query, specify a list of one or more comma-separated actions.
Each action is an operation on a column.

The following actions are supported:

```sql
ADD COLUMN name [type] [default_expr] [AFTER name_after]
```

Adds a new column to the table with the specified name, type, and `default_expr` (see the section "Default expressions"). If you specify `AFTER name_after` (the name of another column), the column is added after the specified one in the list of table columns. Otherwise, the column is added to the end of the table. Note that there is no way to add a column to the beginning of a table. For a chain of actions, 'name_after' can be the name of a column that is added in one of the previous actions.

Adding a column just changes the table structure, without performing any actions with data. The data doesn't appear on the disk after ALTER. If the data is missing for a column when reading from the table, it is filled in with default values (by performing the default expression if there is one, or using zeros or empty strings). If the data is missing for a column when reading from the table, it is filled in with default values (by performing the default expression if there is one, or using zeros or empty strings). The column appears on the disk after merging data parts (see MergeTree).

This approach allows us to complete the ALTER query instantly, without increasing the volume of old data.

```sql
DROP COLUMN name
```

Deletes the column with the name 'name'.
Deletes data from the file system. Since this deletes entire files, the query is completed almost instantly.

```sql
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

There is no support for changing the column type in arrays and nested data structures.

The `ALTER` query lets you create and delete separate elements (columns) in nested data structures, but not whole nested data structures. To add a nested data structure, you can add columns with a name like `name.nested_name` and the type `Array(T)`. A nested data structure is equivalent to multiple array columns with a name that has the same prefix before the dot.

There is no support for deleting columns in the primary key or the sampling key (columns that are in the `ENGINE` expression). Changing the type for columns that are included in the primary key is only possible if this change does not cause the data to be modified (for example, it is allowed to add values to an Enum or change a type with `DateTime`  to `UInt32`).

If the `ALTER` query is not sufficient for making the table changes you need, you can create a new table, copy the data to it using the `INSERT SELECT` query, then switch the tables using the `RENAME` query and delete the old table.

The `ALTER` query blocks all reads and writes for the table. In other words, if a long `SELECT` is running at the time of the `ALTER` query, the `ALTER` query will wait for it to complete. At the same time, all new queries to the same table will wait while this `ALTER` is running.

For tables that don't store data themselves (such as `Merge` and `Distributed`), `ALTER` just changes the table structure, and does not change the structure of subordinate tables. For example, when running ALTER for a `Distributed` table, you will also need to run `ALTER` for the tables on all remote servers.

The `ALTER` query for changing columns is replicated. The instructions are saved in ZooKeeper, then each replica applies them. All `ALTER` queries are run in the same order. The query waits for the appropriate actions to be completed on the other replicas. However, a query to change columns in a replicated table can be interrupted, and all actions will be performed asynchronously.

### Manipulations with partitions and parts

It only works for tables in the `MergeTree` family. The following operations are available:

- `DETACH PARTITION` – Move a partition to the 'detached' directory and forget it.
- `DROP PARTITION` – Delete a partition.
- `ATTACH PART|PARTITION` – Add a new part or partition from the `detached` directory to the table.
- `FREEZE PARTITION` – Create a backup of a partition.
- `FETCH PARTITION` – Download a partition from another server.

Each type of query is covered separately below.

A partition in a table is data for a single calendar month. This is determined by the values of the date key specified in the table engine parameters. Each month's data is stored separately in order to simplify manipulations with this data.

A "part" in the table is part of the data from a single partition, sorted by the primary key.

You can use the `system.parts` table to view the set of table parts and partitions:

```sql
SELECT * FROM system.parts WHERE active
```

`active` – Only count active parts. Inactive parts are, for example, source parts remaining after merging to a larger part – these parts are deleted approximately 10 minutes after merging.

Another way to view a set of parts and partitions is to go into the directory with table data.
Data directory: `/var/lib/clickhouse/data/database/table/`,
where `/var/lib/clickhouse/` is the path to the ClickHouse data, 'database' is the database name, and 'table' is the table name. Example:

```bash
$ ls -l /var/lib/clickhouse/data/test/visits/
total 48
drwxrwxrwx 2 clickhouse clickhouse 20480 may   13 02:58 20140317_20140323_2_2_0
drwxrwxrwx 2 clickhouse clickhouse 20480 may   13 02:58 20140317_20140323_4_4_0
drwxrwxrwx 2 clickhouse clickhouse  4096 may   13 02:55 detached
-rw-rw-rw- 1 clickhouse clickhouse     2 may   13 02:58 increment.txt
```

Here, `20140317_20140323_2_2_0` and ` 20140317_20140323_4_4_0` are the directories of data parts.

Let's break down the name of the first part: `20140317_20140323_2_2_0`.

- `20140317` is the minimum date of the data in the chunk.
- `20140323` is the maximum date of the data in the chunk.
- `2` is the minimum number of the data block.
- `2` is the maximum number of the data block.
- `0` is the chunk level (the depth of the merge tree it is formed from).

Each piece relates to a single partition and contains data for just one month.
`201403` is the name of the partition. A partition is a set of parts for a single month.

On an operating server, you can't manually change the set of parts or their data on the file system, since the server won't know about it.
For non-replicated tables, you can do this when the server is stopped, but we don't recommended it.
For replicated tables, the set of parts can't be changed in any case.

The `detached` directory contains parts that are not used by the server - detached from the table using the `ALTER ... DETACH` query. Parts that are damaged are also moved to this directory, instead of deleting them. You can add, delete, or modify the data in the 'detached' directory at any time – the server won't know about this until you make the `ALTER TABLE ... ATTACH` query.

```sql
ALTER TABLE [db.]table DETACH PARTITION 'name'
```

Move all data for partitions named 'name' to the 'detached' directory and forget about them.
The partition name is specified in YYYYMM format. It can be indicated in single quotes or without them.

After the query is executed, you can do whatever you want with the data in the 'detached' directory — delete it from the file system, or just leave it.

The query is replicated – data will be moved to the 'detached' directory and forgotten on all replicas. The query can only be sent to a leader replica. To find out if a replica is a leader, perform SELECT to the 'system.replicas' system table. Alternatively, it is easier to make a query on all replicas, and all except one will throw an exception.

```sql
ALTER TABLE [db.]table DROP PARTITION 'name'
```

The same as the `DETACH` operation. Deletes data from the table. Data parts will be tagged as inactive and will be completely deleted in approximately 10 minutes. The query is replicated – data will be deleted on all replicas.

```sql
ALTER TABLE [db.]table ATTACH PARTITION|PART 'name'
```

Adds data to the table from the 'detached' directory.

It is possible to add data for an entire partition or a separate part. For a part, specify the full name of the part in single quotes.

The query is replicated. Each replica checks whether there is data in the 'detached' directory. If there is data, it checks the integrity, verifies that it matches the data on the server that initiated the query, and then adds it if everything is correct. If not, it downloads data from the query requestor replica, or from another replica where the data has already been added.

So you can put data in the 'detached' directory on one replica, and use the ALTER ... ATTACH query to add it to the table on all replicas.

```sql
ALTER TABLE [db.]table FREEZE PARTITION 'name'
```

Creates a local backup of one or multiple partitions. The name can be the full name of the partition (for example, 201403), or its prefix (for example, 2014): then the backup will be created for all the corresponding partitions.

The query does the following: for a data snapshot at the time of execution, it creates hardlinks to table data in the directory `/var/lib/clickhouse/shadow/N/...`

`/var/lib/clickhouse/` is the working ClickHouse directory from the config.
`N` is the incremental number of the backup.

The same structure of directories is created inside the backup as inside `/var/lib/clickhouse/`.
It also performs 'chmod' for all files, forbidding writes to them.

The backup is created almost instantly (but first it waits for current queries to the corresponding table to finish running). At first, the backup doesn't take any space on the disk. As the system works, the backup can take disk space, as data is modified. If the backup is made for old enough data, it won't take space on the disk.

After creating the backup, data from `/var/lib/clickhouse/shadow/` can be copied to the remote server and then deleted on the local server.
The entire backup process is performed without stopping the server.

The `ALTER ... FREEZE PARTITION` query is not replicated. A local backup is only created on the local server.

As an alternative, you can manually copy data from the `/var/lib/clickhouse/data/database/table` directory.
But if you do this while the server is running, race conditions are possible when copying directories with files being added or changed, and the backup may be inconsistent. You can do this if the server isn't running – then the resulting data will be the same as after the `ALTER TABLE t FREEZE PARTITION` query.

`ALTER TABLE ... FREEZE PARTITION` only copies data, not table metadata. To make a backup of table metadata, copy the file  `/var/lib/clickhouse/metadata/database/table.sql`

To restore from a backup:

> - Use the CREATE query to create the table if it doesn't exist. The query can be taken from an .sql file (replace `ATTACH` in it with `CREATE`).
- Copy the data from the data/database/table/ directory inside the backup to the `/var/lib/clickhouse/data/database/table/detached/ directory.`
- Run `ALTER TABLE ... ATTACH PARTITION YYYYMM` queries, where `YYYYMM` is the month, for every month.

In this way, data from the backup will be added to the table.
Restoring from a backup doesn't require stopping the server.

### Backups and replication

Replication provides protection from device failures. If all data disappeared on one of your replicas, follow the instructions in the "Restoration after failure" section to restore it.

For protection from device failures, you must use replication. For more information about replication, see the section "Data replication".

Backups protect against human error (accidentally deleting data, deleting the wrong data or in the wrong cluster, or corrupting data).
For high-volume databases, it can be difficult to copy backups to remote servers. In such cases, to protect from human error, you can keep a backup on the same server (it will reside in `/var/lib/clickhouse/shadow/`).

```sql
ALTER TABLE [db.]table FETCH PARTITION 'name' FROM 'path-in-zookeeper'
```

This query only works for replicatable tables.

It downloads the specified partition from the shard that has its `ZooKeeper path` specified in the `FROM` clause, then puts it in the `detached` directory for the specified table.

Although the query is called `ALTER TABLE`, it does not change the table structure, and does not immediately change the data available in the table.

Data is placed in the `detached` directory. You can use the `ALTER TABLE ... ATTACH` query to attach the data.

The ` FROM`  clause specifies the path in ` ZooKeeper`. For example, `/clickhouse/tables/01-01/visits`.
Before downloading, the system checks that the partition exists and the table structure matches. The most appropriate replica is selected automatically from the healthy replicas.

The `ALTER ... FETCH PARTITION` query is not replicated. The partition will be downloaded to the 'detached' directory only on the local server. Note that if after this you use the `ALTER TABLE ... ATTACH` query to add data to the table, the data will be added on all replicas (on one of the replicas it will be added from the 'detached' directory, and on the rest it will be loaded from neighboring replicas).

### Synchronicity of ALTER queries

For non-replicatable tables, all `ALTER` queries are performed synchronously. For replicatable tables, the query just adds instructions for the appropriate actions to `ZooKeeper`, and the actions themselves are performed as soon as possible. However, the query can wait for these actions to be completed on all the replicas.

For `ALTER ... ATTACH|DETACH|DROP` queries, you can use the `replication_alter_partitions_sync` setting to set up waiting.
Possible values: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

<a name="query_language_queries_show_databases"></a>

## SHOW DATABASES

```sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

Prints a list of all databases.
This query is identical to `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

See also the section "Formats".

## SHOW TABLES

```sql
SHOW [TEMPORARY] TABLES [FROM db] [LIKE 'pattern'] [INTO OUTFILE filename] [FORMAT format]
```

Displays a list of tables

- tables from the current database, or from the 'db' database if "FROM db" is specified.
- all tables, or tables whose name matches the pattern, if "LIKE 'pattern'" is specified.

This query is identical to: `SELECT name FROM system.tables WHERE database = 'db' [AND name LIKE 'pattern'] [INTO OUTFILE filename] [FORMAT format]`.

See the section "LIKE operator" also.

## SHOW PROCESSLIST

```sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

Outputs a list of queries currently being processed, other than `SHOW PROCESSLIST` queries.

Prints a table containing the columns:

**user** – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the 'default' user. SHOW PROCESSLIST shows the username for a specific query, not for a query that this query initiated.

**address** – The name of the host that the query was sent from. For distributed processing, on remote servers, this is the name of the query requestor host. To track where a distributed query was originally made from, look at SHOW PROCESSLIST on the query requestor server.

**elapsed** – The execution time, in seconds. Queries are output in order of decreasing execution time.

**rows_read**, **bytes_read** – How many rows and bytes of uncompressed data were read when processing the query. For distributed processing, data is totaled from all the remote servers. This is the data used for restrictions and quotas.

**memory_usage** – Current RAM usage in bytes. See the setting 'max_memory_usage'.

**query** – The query itself. In INSERT queries, the data for insertion is not output.

**query_id** - The query identifier. Non-empty only if it was explicitly defined by the user. For distributed processing, the query ID is not passed to remote servers.

This query is identical to: `SELECT * FROM system.processes [INTO OUTFILE filename] [FORMAT format]`.

Tip (execute in the console):

```bash
watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW CREATE TABLE

```sql
SHOW CREATE [TEMPORARY] TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Returns a single `String`-type 'statement' column, which contains a single value – the `CREATE` query used for creating the specified table.

## DESCRIBE TABLE

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Returns two `String`-type columns: `name` and `type`, which indicate the names and types of columns in the specified table.

Nested data structures are output in "expanded" format. Each column is shown separately, with the name after a dot.

## EXISTS

```sql
EXISTS [TEMPORARY] TABLE [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Returns a single `UInt8`-type column, which contains the single value `0` if the table or database doesn't exist, or `1` if the table exists in the specified database.

## USE

```sql
USE db
```

Lets you set the current database for the session.
The current database is used for searching for tables if the database is not explicitly defined in the query with a dot before the table name.
This query can't be made when using the HTTP protocol, since there is no concept of a session.

## SET

```sql
SET param = value
```

Allows you to set `param` to `value`. You can also make all the settings from the specified settings profile in a single query. To do this, specify 'profile' as the setting name. For more information, see the section "Settings".
The setting is made for the session, or for the server (globally) if `GLOBAL` is specified.
When making a global setting, the setting is not applied to sessions already running, including the current session. It will only be used for new sessions.

When the server is restarted, global settings made using `SET` are lost.
To make settings that persist after a server restart, you can only use the server's config file.

## OPTIMIZE

```sql
OPTIMIZE TABLE [db.]name [PARTITION partition] [FINAL]
```

Asks the table engine to do something for optimization.
Supported only by `*MergeTree` engines, in which this query initializes a non-scheduled merge of data parts.
If you specify a `PARTITION`, only the specified partition will be optimized.
If you specify `FINAL`, optimization will be performed even when all the data is already in one part.

<a name="queries-insert"></a>

## INSERT

Adding data.

Basic query format:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

The query can specify a list of columns to insert `[(c1, c2, c3)]`. In this case, the rest of the columns are filled with:

- The values calculated from the `DEFAULT`  expressions specified in the table definition.
- Zeros and empty strings, if `DEFAULT` expressions are not defined.

If [strict_insert_defaults=1](../operations/settings/settings.md#settings-strict_insert_defaults), columns that do not have ` DEFAULT` defined must be listed in the query.

The INSERT can pass data in any [format](../formats/index.md#formats) supported by ClickHouse. The format must be specified explicitly in the query:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of INSERT ... VALUES:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse removes all spaces and one line feed (if there is one) before the data. When forming a query, we recommend putting the data on a new line after the query operators (this is important if the data begins with spaces).

Example:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

You can insert data separately from the query by using the command-line client or the HTTP interface. For more information, see the section "[Interfaces](../interfaces/index.md#interfaces)".

### Inserting the results of `SELECT`

```sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

Columns are mapped according to their position in the SELECT clause. However, their names in the SELECT expression and the table for INSERT may differ. If necessary, type casting is performed.

None of the data formats except Values allow setting values to expressions such as `now()`, `1 + 2`,  and so on. The Values format allows limited use of expressions, but this is not recommended, because in this case inefficient code is used for their execution.

Other queries for modifying data parts are not supported: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
However, you can delete old data using `ALTER TABLE ... DROP PARTITION`.

### Performance considerations

`INSERT` sorts the input data by primary key and splits them into partitions by month. If you insert data for mixed months, it can significantly reduce the performance of the `INSERT` query. To avoid this:

- Add data in fairly large batches, such as 100,000 rows at a time.
- Group data by month before uploading it to ClickHouse.

Performance will not decrease if:

- Data is added in real time.
- You upload data that is usually sorted by time.

## SELECT

Data sampling.

```sql
SELECT [DISTINCT] expr_list
    [FROM [db.]table | (subquery) | table_function] [FINAL]
    [SAMPLE sample_coeff]
    [ARRAY JOIN ...]
    [GLOBAL] ANY|ALL INNER|LEFT JOIN (subquery)|table USING columns_list
    [PREWHERE expr]
    [WHERE expr]
    [GROUP BY expr_list] [WITH TOTALS]
    [HAVING expr]
    [ORDER BY expr_list]
    [LIMIT [n, ]m]
    [UNION ALL ...]
    [INTO OUTFILE filename]
    [FORMAT format]
    [LIMIT n BY columns]
```

All the clauses are optional, except for the required list of expressions immediately after SELECT.
The clauses below are described in almost the same order as in the query execution conveyor.

If the query omits the `DISTINCT`, `GROUP BY` and `ORDER BY` clauses and the `IN` and `JOIN` subqueries, the query will be completely stream processed, using O(1) amount of RAM.
Otherwise, the query might consume a lot of RAM if the appropriate restrictions are not specified: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. For more information, see the section "Settings". It is possible to use external sorting (saving temporary tables to a disk) and external aggregation. `The system does not have "merge join"`.

### FROM clause

If the FROM clause is omitted, data will be read from the `system.one` table.
The 'system.one' table contains exactly one row (this table fulfills the same purpose as the DUAL table found in other DBMSs).

The FROM clause specifies the table to read data from, or a subquery, or a table function; ARRAY JOIN and the regular JOIN may also be included (see below).

Instead of a table, the SELECT subquery may be specified in brackets.
In this case, the subquery processing pipeline will be built into the processing pipeline of an external query.
In contrast to standard SQL, a synonym does not need to be specified after a subquery. For compatibility, it is possible to write 'AS name' after a subquery, but the specified name isn't used anywhere.

A table function may be specified instead of a table. For more information, see the section "Table functions".

To execute a query, all the columns listed in the query are extracted from the appropriate table. Any columns not needed for the external query are thrown out of the subqueries.
If a query does not list any columns (for example, SELECT count() FROM t), some column is extracted from the table anyway (the smallest one is preferred), in order to calculate the number of rows.

The FINAL modifier can be used only for a SELECT from a CollapsingMergeTree table. When you specify FINAL, data is selected fully "collapsed". Keep in mind that using FINAL leads to a selection that includes columns related to the primary key, in addition to the columns specified in the SELECT. Additionally, the query will be executed in a single stream, and data will be merged during query execution. This means that when using FINAL, the query is processed more slowly. In most cases, you should avoid using FINAL. For more information, see the section "CollapsingMergeTree engine".

### SAMPLE clause

The SAMPLE clause allows for approximated query processing. Approximated query processing is only supported by MergeTree\* type tables, and only if the sampling expression was specified during table creation (see the section "MergeTree engine").

`SAMPLE` has the `format SAMPLE k`, where `k` is a decimal number from 0 to 1, or `SAMPLE n`, where 'n' is a sufficiently large integer.

In the first case, the query will be executed on 'k' percent of data. For example, `SAMPLE 0.1` runs the query on 10% of data.
In the second case, the query will be executed on a sample of no more than 'n' rows. For example, `SAMPLE 10000000` runs the query on a maximum of 10,000,000 rows.

Example:

```sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
    AND toDate(EventDate) >= toDate('2013-01-29')
    AND toDate(EventDate) <= toDate('2013-02-04')
    AND NOT DontCountHits
    AND NOT Refresh
    AND Title != ''
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

In this example, the query is executed on a sample from 0.1 (10%) of data. Values of aggregate functions are not corrected automatically, so to get an approximate result, the value 'count()' is manually multiplied by 10.

When using something like `SAMPLE 10000000`, there isn't any information about which relative percent of data was processed or what the aggregate functions should be multiplied by, so this method of writing is not always appropriate to the situation.

A sample with a relative coefficient is "consistent": if we look at all possible data that could be in the table, a sample (when using a single sampling expression specified during table creation) with the same coefficient always selects the same subset of possible data. In other words, a sample from different tables on different servers at different times is made the same way.

For example, a sample of user IDs takes rows with the same subset of all the possible user IDs from different tables. This allows using the sample in subqueries in the IN clause, as well as for manually correlating results of different queries with samples.

### ARRAY JOIN clause

Allows executing JOIN with an array or nested data structure. The intent is similar to the 'arrayJoin' function, but its functionality is broader.

`ARRAY JOIN` is essentially `INNER JOIN` with an array. Example:

```text
:) CREATE TABLE arrays_test (s String, arr Array(UInt8)) ENGINE = Memory

CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory

Ok.

0 rows in set. Elapsed: 0.001 sec.

:) INSERT INTO arrays_test VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', [])

INSERT INTO arrays_test VALUES

Ok.

3 rows in set. Elapsed: 0.001 sec.

:) SELECT * FROM arrays_test

SELECT *
FROM arrays_test

┌─s───────┬─arr─────┐
│ Hello   │ [1,2]   │
│ World   │ [3,4,5] │
│ Goodbye │ []      │
└─────────┴─────────┘

3 rows in set. Elapsed: 0.001 sec.

:) SELECT s, arr FROM arrays_test ARRAY JOIN arr

SELECT s, arr
FROM arrays_test
ARRAY JOIN arr

┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘

5 rows in set. Elapsed: 0.001 sec.
```

An alias can be specified for an array in the ARRAY JOIN clause. In this case, an array item can be accessed by this alias, but the array itself by the original name. Example:

```text
:) SELECT s, arr, a FROM arrays_test ARRAY JOIN arr AS a

SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a

┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘

5 rows in set. Elapsed: 0.001 sec.
```

Multiple arrays of the same size can be comma-separated in the ARRAY JOIN clause. In this case, JOIN is performed with them simultaneously (the direct sum, not the direct product). Example:

```text
:) SELECT s, arr, a, num, mapped FROM arrays_test ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped

SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(lambda(tuple(x), plus(x, 1)), arr) AS mapped

┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘

5 rows in set. Elapsed: 0.002 sec.

:) SELECT s, arr, a, num, arrayEnumerate(arr) FROM arrays_test ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num

SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num

┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘

5 rows in set. Elapsed: 0.002 sec.
```

ARRAY JOIN also works with nested data structures. Example:

```text
:) CREATE TABLE nested_test (s String, nest Nested(x UInt8, y UInt32)) ENGINE = Memory

CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory

Ok.

0 rows in set. Elapsed: 0.006 sec.

:) INSERT INTO nested_test VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], [])

INSERT INTO nested_test VALUES

Ok.

3 rows in set. Elapsed: 0.001 sec.

:) SELECT * FROM nested_test

SELECT *
FROM nested_test

┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘

3 rows in set. Elapsed: 0.001 sec.

:) SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest

SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest

┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘

5 rows in set. Elapsed: 0.001 sec.
```

When specifying names of nested data structures in ARRAY JOIN, the meaning is the same as ARRAY JOIN with all the array elements that it consists of. Example:

```text
:) SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest.x, nest.y

SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`

┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘

5 rows in set. Elapsed: 0.001 sec.
```

This variation also makes sense:

```text
:) SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest.x

SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`

┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘

5 rows in set. Elapsed: 0.001 sec.
```

An alias may be used for a nested data structure, in order to select either the JOIN result or the source array. Example:

```text
:) SELECT s, n.x, n.y, nest.x, nest.y FROM nested_test ARRAY JOIN nest AS n

SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n

┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘

5 rows in set. Elapsed: 0.001 sec.
```

Example of using the arrayEnumerate function:

```text
:) SELECT s, n.x, n.y, nest.x, nest.y, num FROM nested_test ARRAY JOIN nest AS n, arrayEnumerate(nest.x) AS num

SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num

┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘

5 rows in set. Elapsed: 0.002 sec.
```

The query can only specify a single ARRAY JOIN clause.

The corresponding conversion can be performed before the WHERE/PREWHERE clause (if its result is needed in this clause), or after completing WHERE/PREWHERE (to reduce the volume of calculations).

### JOIN clause

The normal JOIN, which is not related to ARRAY JOIN described above.

```sql
[GLOBAL] ANY|ALL INNER|LEFT [OUTER] JOIN (subquery)|table USING columns_list
```

Performs joins with data from the subquery. At the beginning of query processing, the subquery specified after JOIN is run, and its result is saved in memory. Then it is read from the "left" table specified in the FROM clause, and while it is being read, for each of the read rows from the "left" table, rows are selected from the subquery results table (the "right" table) that meet the condition for matching the values of the columns specified in USING.

The table name can be specified instead of a subquery. This is equivalent to the `SELECT * FROM table` subquery, except in a special case when the table has the Join engine – an array prepared for joining.

All columns that are not needed for the JOIN are deleted from the subquery.

There are several types of JOINs:

`INNER` or `LEFT` type:
If INNER is specified, the result will contain only those rows that have a matching row in the right table.
If LEFT is specified, any rows in the left table that don't have matching rows in the right table will be assigned the default value - zeros or empty rows. LEFT OUTER may be written instead of LEFT; the word OUTER does not affect anything.

`ANY` or `ALL` stringency:If `ANY` is specified and the right table has several matching rows, only the first one found is joined.
If `ALL` is specified and the right table has several matching rows, the data will be multiplied by the number of these rows.

Using ALL corresponds to the normal JOIN semantic from standard SQL.
Using ANY is optimal. If the right table has only one matching row, the results of ANY and ALL are the same. You must specify either ANY or ALL (neither of them is selected by default).

`GLOBAL` distribution:

When using a normal JOIN, the query is sent to remote servers. Subqueries are run on each of them in order to make the right table, and the join is performed with this table. In other words, the right table is formed on each server separately.

When using `GLOBAL ... JOIN`, first the requestor server runs a subquery to calculate the right table. This temporary table is passed to each remote server, and queries are run on them using the temporary data that was transmitted.

Be careful when using GLOBAL JOINs. For more information, see the section "Distributed subqueries".

Any combination of JOINs is possible. For example, `GLOBAL ANY LEFT OUTER JOIN`.

When running a JOIN, there is no optimization of the order of execution in relation to other stages of the query. The join (a search in the right table) is run before filtering in WHERE and before aggregation. In order to explicitly set the processing order, we recommend running a JOIN subquery with a subquery.

Example:

```sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

```text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

Subqueries don't allow you to set names or use them for referencing a column from a specific subquery.
The columns specified in USING must have the same names in both subqueries, and the other columns must be named differently. You can use aliases to change the names of columns in subqueries (the example uses the aliases 'hits' and 'visits').

The USING clause specifies one or more columns to join, which establishes the equality of these columns. The list of columns is set without brackets. More complex join conditions are not supported.

The right table (the subquery result) resides in RAM. If there isn't enough memory, you can't run a JOIN.

Only one JOIN can be specified in a query (on a single level). To run multiple JOINs, you can put them in subqueries.

Each time a query is run with the same JOIN, the subquery is run again – the result is not cached. To avoid this, use the special 'Join' table engine, which is a prepared array for joining that is always in RAM. For more information, see the section "Table engines, Join".

In some cases, it is more efficient to use IN instead of JOIN.
Among the various types of JOINs, the most efficient is ANY LEFT JOIN, then ANY INNER JOIN. The least efficient are ALL LEFT JOIN and ALL INNER JOIN.

If you need a JOIN for joining with dimension tables (these are relatively small tables that contain dimension properties, such as names for advertising campaigns), a JOIN might not be very convenient due to the bulky syntax and the fact that the right table is re-accessed for every query. For such cases, there is an "external dictionaries" feature that you should use instead of JOIN. For more information, see the section "External dictionaries".

### WHERE clause

If there is a WHERE clause, it must contain an expression with the UInt8 type. This is usually an expression with comparison and logical operators.
This expression will be used for filtering data before all other transformations.

If indexes are supported by the database table engine, the expression is evaluated on the ability to use indexes.

### PREWHERE clause

This clause has the same meaning as the WHERE clause. The difference is in which data is read from the table.
When using PREWHERE, first only the columns necessary for executing PREWHERE are read. Then the other columns are read that are needed for running the query, but only those blocks where the PREWHERE expression is true.

It makes sense to use PREWHERE if there are filtration conditions that are not suitable for indexes that are used by a minority of the columns in the query, but that provide strong data filtration. This reduces the volume of data to read.

For example, it is useful to write PREWHERE for queries that extract a large number of columns, but that only have filtration for a few columns.

PREWHERE is only supported by tables from the `*MergeTree` family.

A query may simultaneously specify PREWHERE and WHERE. In this case, PREWHERE precedes WHERE.

Keep in mind that it does not make much sense for PREWHERE to only specify those columns that have an index, because when using an index, only the data blocks that match the index are read.

If the 'optimize_move_to_prewhere' setting is set to 1 and PREWHERE is omitted, the system uses heuristics to automatically move parts of expressions from WHERE to PREWHERE.

### GROUP BY clause

This is one of the most important parts of a column-oriented DBMS.

If there is a GROUP BY clause, it must contain a list of expressions. Each expression will be referred to here as a "key".
All the expressions in the SELECT, HAVING, and ORDER BY clauses must be calculated from keys or from aggregate functions. In other words, each column selected from the table must be used either in keys or inside aggregate functions.

If a query contains only table columns inside aggregate functions, the GROUP BY clause can be omitted, and aggregation by an empty set of keys is assumed.

Example:

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

However, in contrast to standard SQL, if the table doesn't have any rows (either there aren't any at all, or there aren't any after using WHERE to filter), an empty result is returned, and not the result from one of the rows containing the initial values of aggregate functions.

As opposed to MySQL (and conforming to standard SQL), you can't get some value of some column that is not in a key or aggregate function (except constant expressions). To work around this, you can use the 'any' aggregate function (get the first encountered value) or 'min/max'.

Example:

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

For every different key value encountered, GROUP BY calculates a set of aggregate function values.

GROUP BY is not supported for array columns.

A constant can't be specified as arguments for aggregate functions. Example: sum(1). Instead of this, you can get rid of the constant. Example: `count()`.

#### WITH TOTALS modifier

If the WITH TOTALS modifier is specified, another row will be calculated. This row will have key columns containing default values (zeros or empty lines), and columns of aggregate functions with the values calculated across all the rows (the "total" values).

This extra row is output in JSON\*, TabSeparated\*, and Pretty\* formats, separately from the other rows. In the other formats, this row is not output.

In JSON\* formats, this row is output as a separate 'totals' field. In TabSeparated\* formats, the row comes after the main result, preceded by an empty row (after the other data). In Pretty\* formats, the row is output as a separate table after the main result.

`WITH TOTALS` can be run in different ways when HAVING is present. The behavior depends on the 'totals_mode' setting.
By default, `totals_mode = 'before_having'`. In this case, 'totals' is calculated across all rows, including the ones that don't pass through HAVING and 'max_rows_to_group_by'.

The other alternatives include only the rows that pass through HAVING in 'totals', and behave differently with the setting `max_rows_to_group_by` and `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. In other words, 'totals' will have less than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_inclusive` – Include all the rows that didn't pass through 'max_rows_to_group_by' in 'totals'. In other words, 'totals' will have more than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through 'max_rows_to_group_by' in 'totals'. Otherwise, do not include them.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

If `max_rows_to_group_by` and `group_by_overflow_mode = 'any'` are not used, all variations of `after_having` are the same, and you can use any of them (for example, `after_having_auto`).

You can use WITH TOTALS in subqueries, including subqueries in the JOIN clause (in this case, the respective total values are combined).

#### GROUP BY in external memory

You can enable dumping temporary data to the disk to restrict memory usage during GROUP BY.
The `max_bytes_before_external_group_by` setting determines the threshold RAM consumption for dumping GROUP BY temporary data to the file system. If set to 0 (the default), it is disabled.

When using `max_bytes_before_external_group_by`, we recommend that you set max_memory_usage about twice as high. This is necessary because there are two stages to aggregation: reading the date and forming intermediate data (1) and merging the intermediate data (2). Dumping data to the file system can only occur during stage 1. If the temporary data wasn't dumped, then stage 2 might require up to the same amount of memory as in stage 1.

For example, if `max_memory_usage` was set to 10000000000 and you want to use external aggregation, it makes sense to set `max_bytes_before_external_group_by` to 10000000000, and max_memory_usage to 20000000000. When external aggregation is triggered (if there was at least one dump of temporary data), maximum consumption of RAM is only slightly more than ` max_bytes_before_external_group_by`.

With distributed query processing, external aggregation is performed on remote servers. In order for the requestor server to use only a small amount of RAM, set ` distributed_aggregation_memory_efficient`  to 1.

When merging data flushed to the disk, as well as when merging results from remote servers when the ` distributed_aggregation_memory_efficient` setting is enabled, consumes up to 1/256 \* the number of threads from the total amount of RAM.

When external aggregation is enabled, if there was less than ` max_bytes_before_external_group_by`  of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

If you have an ORDER BY with a small LIMIT after GROUP BY, then the ORDER BY CLAUSE will not use significant amounts of RAM.
But if the ORDER BY doesn't have LIMIT, don't forget to enable external sorting (`max_bytes_before_external_sort`).

### LIMIT N BY clause

LIMIT N BY COLUMNS selects the top N rows for each group of COLUMNS. LIMIT N BY is not related to LIMIT; they can both be used in the same query. The key for LIMIT N BY can contain any number of columns or expressions.

Example:

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

The query will select the top 5 referrers for each `domain, device_type` pair, but not more than 100 rows (`LIMIT n BY + LIMIT`).

### HAVING clause

Allows filtering the result received after GROUP BY, similar to the WHERE clause.
WHERE and HAVING differ in that WHERE is performed before aggregation (GROUP BY), while HAVING is performed after it.
If aggregation is not performed, HAVING can't be used.

<a name="query_language-queries-order_by"></a>

### ORDER BY clause

The ORDER BY clause contains a list of expressions, which can each be assigned DESC or ASC (the sorting direction). If the direction is not specified, ASC is assumed. ASC is sorted in ascending order, and DESC in descending order. The sorting direction applies to a single expression, not to the entire list. Example: `ORDER BY Visits DESC, SearchPhrase`

For sorting by String values, you can specify collation (comparison). Example: `ORDER BY SearchPhrase COLLATE 'tr'` - for sorting by keyword in ascending order, using the Turkish alphabet, case insensitive, assuming that strings are UTF-8 encoded. COLLATE can be specified or not for each expression in ORDER BY independently. If ASC or DESC is specified, COLLATE is specified after it. When using COLLATE, sorting is always case-insensitive.

We only recommend using COLLATE for final sorting of a small number of rows, since sorting with COLLATE is less efficient than normal sorting by bytes.

Rows that have identical values for the list of sorting expressions are output in an arbitrary order, which can also be nondeterministic (different each time).
If the ORDER BY clause is omitted, the order of the rows is also undefined, and may be nondeterministic as well.

When floating point numbers are sorted, NaNs are separate from the other values. Regardless of the sorting order, NaNs come at the end. In other words, for ascending sorting they are placed as if they are larger than all the other numbers, while for descending sorting they are placed as if they are smaller than the rest.

Less RAM is used if a small enough LIMIT is specified in addition to ORDER BY. Otherwise, the amount of memory spent is proportional to the volume of data for sorting. For distributed query processing, if GROUP BY is omitted, sorting is partially done on remote servers, and the results are merged on the requestor server. This means that for distributed sorting, the volume of data to sort can be greater than the amount of memory on a single server.

If there is not enough RAM, it is possible to perform sorting in external memory (creating temporary files on a disk). Use the setting `max_bytes_before_external_sort` for this purpose. If it is set to 0 (the default), external sorting is disabled. If it is enabled, when the volume of data to sort reaches the specified number of bytes, the collected data is sorted and dumped into a temporary file. After all data is read, all the sorted files are merged and the results are output. Files are written to the /var/lib/clickhouse/tmp/ directory in the config (by default, but you can use the 'tmp_path' parameter to change this setting).

Running a query may use more memory than 'max_bytes_before_external_sort'. For this reason, this setting must have a value significantly smaller than 'max_memory_usage'. As an example, if your server has 128 GB of RAM and you need to run a single query, set 'max_memory_usage' to 100 GB, and 'max_bytes_before_external_sort' to 80 GB.

External sorting works much less effectively than sorting in RAM.

### SELECT clause

The expressions specified in the SELECT clause are analyzed after the calculations for all the clauses listed above are completed.
More specifically, expressions are analyzed that are above the aggregate functions, if there are any aggregate functions.
The aggregate functions and everything below them are calculated during aggregation (GROUP BY).
These expressions work as if they are applied to separate rows in the result.

### DISTINCT clause

If DISTINCT is specified, only a single row will remain out of all the sets of fully matching rows in the result.
The result will be the same as if GROUP BY were specified across all the fields specified in SELECT without aggregate functions. But there are several differences from GROUP BY:

- DISTINCT can be applied together with GROUP BY.
- When ORDER BY is omitted and LIMIT is defined, the query stops running immediately after the required number of different rows has been read.
- Data blocks are output as they are processed, without waiting for the entire query to finish running.

DISTINCT is not supported if SELECT has at least one array column.

### LIMIT clause

LIMIT m allows you to select the first 'm' rows from the result.
LIMIT n, m allows you to select the first 'm' rows from the result after skipping the first 'n' rows.

'n' and 'm' must be non-negative integers.

If there isn't an ORDER BY clause that explicitly sorts results, the result may be arbitrary and nondeterministic.

### UNION ALL clause

You can use UNION ALL to combine any number of queries. Example:

```sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Only UNION ALL is supported. The regular UNION (UNION DISTINCT) is not supported. If you need UNION DISTINCT, you can write SELECT DISTINCT from a subquery containing UNION ALL.

Queries that are parts of UNION ALL can be run simultaneously, and their results can be mixed together.

The structure of results (the number and type of columns) must match for the queries. But the column names can differ. In this case, the column names for the final result will be taken from the first query.

Queries that are parts of UNION ALL can't be enclosed in brackets. ORDER BY and LIMIT are applied to separate queries, not to the final result. If you need to apply a conversion to the final result, you can put all the queries with UNION ALL in a subquery in the FROM clause.

### INTO OUTFILE clause

Add the `INTO OUTFILE filename` clause (where filename is a string literal) to redirect query output to the specified file.
In contrast to MySQL, the file is created on the client side. The query will fail if a file with the same filename already exists.
This functionality is available in the command-line client and clickhouse-local (a query sent via HTTP interface will fail).

The default output format is TabSeparated (the same as in the command-line client batch mode).

### FORMAT clause

Specify 'FORMAT format' to get data in any specified format.
You can use this for convenience, or for creating dumps.
For more information, see the section "Formats".
If the FORMAT clause is omitted, the default format is used, which depends on both the settings and the interface used for accessing the DB. For the HTTP interface and the command-line client in batch mode, the default format is TabSeparated. For the command-line client in interactive mode, the default format is PrettyCompact (it has attractive and compact tables).

When using the command-line client, data is passed to the client in an internal efficient format. The client independently interprets the FORMAT clause of the query and formats the data itself (thus relieving the network and the server from the load).

### IN operators

The `IN`, `NOT IN`, `GLOBAL IN`, and `GLOBAL NOT IN` operators are covered separately, since their functionality is quite rich.

The left side of the operator is either a single column or a tuple.

Examples:

```sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

If the left side is a single column that is in the index, and the right side is a set of constants, the system uses the index for processing the query.

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section "External data for query processing"), then use a subquery.

The right side of the operator can be a set of constant expressions, a set of tuples with constant expressions (shown in the examples above), or the name of a database table or SELECT subquery in brackets.

If the right side of the operator is the name of a table (for example, `UserID IN users`), this is equivalent to the subquery `UserID IN (SELECT * FROM users)`. Use this when working with external data that is sent along with the query. For example, the query can be sent together with a set of user IDs loaded to the 'users' temporary table, which should be filtered.

If the right side of the operator is a table name that has the Set engine (a prepared data set that is always in RAM), the data set will not be created over again for each query.

The subquery may specify more than one column for filtering tuples.
Example:

```sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

The columns to the left and right of the IN operator should have the same type.

The IN operator and subquery may occur in any part of the query, including in aggregate functions and lambda functions.
Example:

```sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

```text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

For each day after March 17th, count the percentage of pageviews made by users who visited the site on March 17th.
A subquery in the IN clause is always run just one time on a single server. There are no dependent subqueries.

<a name="queries-distributed-subrequests"></a>

#### Distributed subqueries

There are two options for IN-s with subqueries (similar to JOINs): normal `IN`  / ` OIN`  and `IN GLOBAL`  / `GLOBAL JOIN`. They differ in how they are run for distributed query processing.

<div class="admonition attention">

Remember that the algorithms described below may work differently depending on the [](../operations/settings/settings.md#settings-distributed_product_mode) `distributed_product_mode` setting.

</div>

When using the regular IN, the query is sent to remote servers, and each of them runs the subqueries in the `IN` or `JOIN` clause.

When using `GLOBAL IN`  / `GLOBAL JOINs`, first all the subqueries are run for `GLOBAL IN`  / `GLOBAL JOINs`, and the results are collected in temporary tables. Then the temporary tables are sent to each remote server, where the queries are run using this temporary data.

For a non-distributed query, use the regular `IN` / `JOIN`.

Be careful when using subqueries in the  `IN` / `JOIN` clauses for distributed query processing.

Let's look at some examples. Assume that each server in the cluster has a normal **local_table**. Each server also has a **distributed_table** table with the **Distributed** type, which looks at all the servers in the cluster.

For a query to the **distributed_table**, the query will be sent to all the remote servers and run on them using the **local_table**.

For example, the query

```sql
SELECT uniq(UserID) FROM distributed_table
```

will be sent to all remote servers as

```sql
SELECT uniq(UserID) FROM local_table
```

and run on each of them in parallel, until it reaches the stage where intermediate results can be combined. Then the intermediate results will be returned to the requestor server and merged on it, and the final result will be sent to the client.

Now let's examine a query with IN:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

- Calculation of the intersection of audiences of two sites.

This query will be sent to all remote servers as

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

In other words, the data set in the IN clause will be collected on each server independently, only across the data that is stored locally on each of the servers.

This will work correctly and optimally if you are prepared for this case and have spread data across the cluster servers such that the data for a single UserID resides entirely on a single server. In this case, all the necessary data will be available locally on each server. Otherwise, the result will be inaccurate. We refer to this variation of the query as "local IN".

To correct how the query works when data is spread randomly across the cluster servers, you could specify **distributed_table** inside a subquery. The query would look like this:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

This query will be sent to all remote servers as

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

The subquery will begin running on each remote server. Since the subquery uses a distributed table, the subquery that is on each remote server will be resent to every remote server as

```sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

For example, if you have a cluster of 100 servers, executing the entire query will require 10,000 elementary requests, which is generally considered unacceptable.

In such cases, you should always use GLOBAL IN instead of IN. Let's look at how it works for the query

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

The requestor server will run the subquery

```sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

and the result will be put in a temporary table in RAM. Then the request will be sent to each remote server as

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

and the temporary table '_data1' will be sent to every remote server together with the query (the name of the temporary table is implementation-defined).

This is more optimal than using the normal IN. However, keep the following points in mind:

1. When creating a temporary table, data is not made unique. To reduce the volume of data transmitted over the network, specify DISTINCT in the subquery. (You don't need to do this for a normal IN.)
2. The temporary table will be sent to all the remote servers. Transmission does not account for network topology. For example, if 10 remote servers reside in a datacenter that is very remote in relation to the requestor server, the data will be sent 10 times over the channel to the remote datacenter. Try to avoid large data sets when using GLOBAL IN.
3. When transmitting data to remote servers, restrictions on network bandwidth are not configurable. You might overload the network.
4. Try to distribute data across servers so that you don't need to use GLOBAL IN on a regular basis.
5. If you need to use GLOBAL IN often, plan the location of the ClickHouse cluster so that a single group of replicas resides in no more than one data center with a fast network between them, so that a query can be processed entirely within a single data center.

It also makes sense to specify a local table in the `GLOBAL IN` clause, in case this local table is only available on the requestor server and you want to use data from it on remote servers.

### Extreme values

In addition to results, you can also get minimum and maximum values for the results columns. To do this, set the **extremes** setting to 1. Minimums and maximums are calculated for numeric types, dates, and dates with times. For other columns, the default values are output.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in JSON\*, TabSeparated\*, and Pretty\* formats, separate from the other rows. They are not output for other formats.

In JSON\* formats, the extreme values are output in a separate 'extremes' field. In TabSeparated\* formats, the row comes after the main result, and after 'totals' if present. It is preceded by an empty row (after the other data). In Pretty\* formats, the row is output as a separate table after the main result, and after 'totals' if present.

Extreme values are calculated for rows that have passed through LIMIT. However, when using 'LIMIT offset, size', the rows before 'offset' are included in 'extremes'. In stream requests, the result may also include a small number of rows that passed through LIMIT.

### Notes

The `GROUP BY` and `ORDER BY` clauses do not support positional arguments. This contradicts MySQL, but conforms to standard SQL.
For example, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

You can use synonyms (`AS` aliases) in any part of a query.

You can put an asterisk in any part of a query instead of an expression. When the query is analyzed, the asterisk is expanded to a list of all table columns (excluding the `MATERIALIZED` and `ALIAS` columns). There are only a few cases when using an asterisk is justified:

- When creating a table dump.
- For tables containing just a few columns, such as system tables.
- For getting information about what columns are in a table. In this case, set `LIMIT 1`. But it is better to use the `DESC TABLE` query.
- When there is strong filtration on a small number of columns using `PREWHERE`.
- In subqueries (since columns that aren't needed for the external query are excluded from subqueries).

In all other cases, we don't recommend using the asterisk, since it only gives you the drawbacks of a columnar DBMS instead of the advantages. In other words using the asterisk is not recommended.

## KILL QUERY

```sql
KILL QUERY
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Attempts to terminate queries currently running.
The queries to terminate are selected from the system.processes table for which `WHERE` expression is true.

Examples:

```sql
-- Terminates all queries with the specified query_id.
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by `username`.
KILL QUERY WHERE user='username' SYNC
```

Readonly-users can only terminate their own requests.

By default, the asynchronous version of queries is used (`ASYNC`), which doesn't wait for query termination.

The synchronous version (`SYNC`) waits for all queries to be killed and displays information about each process as it terminates.
The response contains the `kill_status` column, which can take the following values:

1. 'finished' – The query terminated successfully.
2. 'waiting' – Waiting for the query to finish after sending it a signal to terminate.
3. The other values ​​explain why the query can't be terminated.

A test query (`TEST`) only checks the user's rights and displays a list of queries to terminate.
