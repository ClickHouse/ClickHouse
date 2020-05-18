---
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

The `ALTER` query is only supported for `*MergeTree` tables, as well as `Merge`and`Distributed`. The query has several variations.

### Column Manipulations {#column-manipulations}

Changing the table structure.

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

In the query, specify a list of one or more comma-separated actions.
Each action is an operation on a column.

The following actions are supported:

-   [ADD COLUMN](#alter_add-column) — Adds a new column to the table.
-   [DROP COLUMN](#alter_drop-column) — Deletes the column.
-   [CLEAR COLUMN](#alter_clear-column) — Resets column values.
-   [COMMENT COLUMN](#alter_comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#alter_modify-column) — Changes column’s type, default expression and TTL.

These actions are described in detail below.

#### ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

Adds a new column to the table with the specified `name`, `type`, [`codec`](create.md#codecs) and `default_expr` (see the section [Default expressions](create.md#create-default-values)).

If the `IF NOT EXISTS` clause is included, the query won’t return an error if the column already exists. If you specify `AFTER name_after` (the name of another column), the column is added after the specified one in the list of table columns. Otherwise, the column is added to the end of the table. Note that there is no way to add a column to the beginning of a table. For a chain of actions, `name_after` can be the name of a column that is added in one of the previous actions.

Adding a column just changes the table structure, without performing any actions with data. The data doesn’t appear on the disk after `ALTER`. If the data is missing for a column when reading from the table, it is filled in with default values (by performing the default expression if there is one, or using zeros or empty strings). The column appears on the disk after merging data parts (see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)).

This approach allows us to complete the `ALTER` query instantly, without increasing the volume of old data.

Example:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

Deletes the column with the name `name`. If the `IF EXISTS` clause is specified, the query won’t return an error if the column doesn’t exist.

Deletes data from the file system. Since this deletes entire files, the query is completed almost instantly.

Example:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Resets all data in a column for a specified partition. Read more about setting the partition name in the section [How to specify the partition expression](#alter-how-to-specify-part-expr).

If the `IF EXISTS` clause is specified, the query won’t return an error if the column doesn’t exist.

Example:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

Adds a comment to the column. If the `IF EXISTS` clause is specified, the query won’t return an error if the column doesn’t exist.

Each column can have one comment. If a comment already exists for the column, a new comment overwrites the previous comment.

Comments are stored in the `comment_expression` column returned by the [DESCRIBE TABLE](misc.md#misc-describe-table) query.

Example:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

#### MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```

This query changes the `name` column properties:

-   Type

-   Default expression

-   TTL

        For examples of columns TTL modifying, see [Column TTL](../engines/table_engines/mergetree_family/mergetree.md#mergetree-column-ttl).

If the `IF EXISTS` clause is specified, the query won’t return an error if the column doesn’t exist.

When changing the type, values are converted as if the [toType](../../sql-reference/functions/type-conversion-functions.md) functions were applied to them. If only the default expression is changed, the query doesn’t do anything complex, and is completed almost instantly.

Example:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

There are several processing stages:

-   Preparing temporary (new) files with modified data.
-   Renaming old files.
-   Renaming the temporary (new) files to the old names.
-   Deleting the old files.

Only the first stage takes time. If there is a failure at this stage, the data is not changed.
If there is a failure during one of the successive stages, data can be restored manually. The exception is if the old files were deleted from the file system but the data for the new files did not get written to the disk and was lost.

The `ALTER` query for changing columns is replicated. The instructions are saved in ZooKeeper, then each replica applies them. All `ALTER` queries are run in the same order. The query waits for the appropriate actions to be completed on the other replicas. However, a query to change columns in a replicated table can be interrupted, and all actions will be performed asynchronously.

#### ALTER Query Limitations {#alter-query-limitations}

The `ALTER` query lets you create and delete separate elements (columns) in nested data structures, but not whole nested data structures. To add a nested data structure, you can add columns with a name like `name.nested_name` and the type `Array(T)`. A nested data structure is equivalent to multiple array columns with a name that has the same prefix before the dot.

There is no support for deleting columns in the primary key or the sampling key (columns that are used in the `ENGINE` expression). Changing the type for columns that are included in the primary key is only possible if this change does not cause the data to be modified (for example, you are allowed to add values to an Enum or to change a type from `DateTime` to `UInt32`).

If the `ALTER` query is not sufficient to make the table changes you need, you can create a new table, copy the data to it using the [INSERT SELECT](insert-into.md#insert_query_insert-select) query, then switch the tables using the [RENAME](misc.md#misc_operations-rename) query and delete the old table. You can use the [clickhouse-copier](../../operations/utilities/clickhouse-copier.md) as an alternative to the `INSERT SELECT` query.

The `ALTER` query blocks all reads and writes for the table. In other words, if a long `SELECT` is running at the time of the `ALTER` query, the `ALTER` query will wait for it to complete. At the same time, all new queries to the same table will wait while this `ALTER` is running.

For tables that don’t store data themselves (such as `Merge` and `Distributed`), `ALTER` just changes the table structure, and does not change the structure of subordinate tables. For example, when running ALTER for a `Distributed` table, you will also need to run `ALTER` for the tables on all remote servers.

### Manipulations with Key Expressions {#manipulations-with-key-expressions}

The following command is supported:

``` sql
MODIFY ORDER BY new_expression
```

It only works for tables in the [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) family (including
[replicated](../../engines/table-engines/mergetree-family/replication.md) tables). The command changes the
[sorting key](../../engines/table-engines/mergetree-family/mergetree.md) of the table
to `new_expression` (an expression or a tuple of expressions). Primary key remains the same.

The command is lightweight in a sense that it only changes metadata. To keep the property that data part
rows are ordered by the sorting key expression you cannot add expressions containing existing columns
to the sorting key (only columns added by the `ADD COLUMN` command in the same `ALTER` query).

### Manipulations with Data Skipping Indices {#manipulations-with-data-skipping-indices}

It only works for tables in the [`*MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) family (including
[replicated](../../engines/table-engines/mergetree-family/replication.md) tables). The following operations
are available:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` - Adds index description to tables metadata.

-   `ALTER TABLE [db].name DROP INDEX name` - Removes index description from tables metadata and deletes index files from disk.

These commands are lightweight in a sense that they only change metadata or remove files.
Also, they are replicated (syncing indices metadata through ZooKeeper).

### Manipulations with Constraints {#manipulations-with-constraints}

See more on [constraints](create.md#constraints)

Constraints could be added or deleted using following syntax:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

Queries will add or remove metadata about constraints from table so they are processed immediately.

Constraint check *will not be executed* on existing data if it was added.

All changes on replicated tables are broadcasting to ZooKeeper so will be applied on other replicas.

### Manipulations with Partitions and Parts {#alter_manipulations-with-partitions}

The following operations with [partitions](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) are available:

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

#### DETACH PARTITION {#alter_detach-partition}

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

This query is replicated – it moves the data to the `detached` directory on all replicas. Note that you can execute this query only on a leader replica. To find out if a replica is a leader, perform the `SELECT` query to the [system.replicas](../../operations/system-tables.md#system_tables-replicas) table. Alternatively, it is easier to make a `DETACH` query on all replicas - all the replicas throw an exception, except the leader replica.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

Deletes the specified partition from the table. This query tags the partition as inactive and deletes data completely, approximately in 10 minutes.

Read about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

The query is replicated – it deletes data on all replicas.

#### DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

Removes the specified part or all parts of the specified partition from `detached`.
Read more about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

#### ATTACH PARTITION\|PART {#alter_attach-partition}

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

#### ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

This query copies the data partition from the `table1` to `table2` adds data to exsisting in the `table2`. Note that data won’t be deleted from `table1`.

For the query to run successfully, the following conditions must be met:

-   Both tables must have the same structure.
-   Both tables must have the same partition key.

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

This query copies the data partition from the `table1` to `table2` and replaces existing partition in the `table2`. Note that data won’t be deleted from `table1`.

For the query to run successfully, the following conditions must be met:

-   Both tables must have the same structure.
-   Both tables must have the same partition key.

#### MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

This query moves the data partition from the `table_source` to `table_dest` with deleting the data from `table_source`.

For the query to run successfully, the following conditions must be met:

-   Both tables must have the same structure.
-   Both tables must have the same partition key.
-   Both tables must be the same engine family (replicated or non-replicated).
-   Both tables must have the same storage policy.

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

This query creates a local backup of a specified partition. If the `PARTITION` clause is omitted, the query creates the backup of all partitions at once.

!!! note "Note"
    The entire backup process is performed without stopping the server.

Note that for old-styled tables you can specify the prefix of the partition name (for example, ‘2019’) - then the query creates the backup for all the corresponding partitions. Read about setting the partition expression in a section [How to specify the partition expression](#alter-how-to-specify-part-expr).

At the time of execution, for a data snapshot, the query creates hardlinks to a table data. Hardlinks are placed in the directory `/var/lib/clickhouse/shadow/N/...`, where:

-   `/var/lib/clickhouse/` is the working ClickHouse directory specified in the config.
-   `N` is the incremental number of the backup.

!!! note "Note"
    If you use [a set of disks for data storage in a table](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes), the `shadow/N` directory appears on every disk, storing data parts that matched by the `PARTITION` expression.

The same structure of directories is created inside the backup as inside `/var/lib/clickhouse/`. The query performs ‘chmod’ for all files, forbidding writing into them.

After creating the backup, you can copy the data from `/var/lib/clickhouse/shadow/` to the remote server and then delete it from the local server. Note that the `ALTER t FREEZE PARTITION` query is not replicated. It creates a local backup only on the local server.

The query creates backup almost instantly (but first it waits for the current queries to the corresponding table to finish running).

`ALTER TABLE t FREEZE PARTITION` copies only the data, not table metadata. To make a backup of table metadata, copy the file `/var/lib/clickhouse/metadata/database/table.sql`

To restore data from a backup, do the following:

1.  Create the table if it does not exist. To view the query, use the .sql file (replace `ATTACH` in it with `CREATE`).
2.  Copy the data from the `data/database/table/` directory inside the backup to the `/var/lib/clickhouse/data/database/table/detached/` directory.
3.  Run `ALTER TABLE t ATTACH PARTITION` queries to add the data to a table.

Restoring from a backup doesn’t require stopping the server.

For more information about backups and restoring data, see the [Data Backup](../../operations/backup.md) section.

#### CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

The query works similar to `CLEAR COLUMN`, but it resets an index instead of a column data.

#### FETCH PARTITION {#alter_fetch-partition}

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

#### MOVE PARTITION\|PART {#alter_move-partition}

Moves partitions or data parts to another volume or disk for `MergeTree`-engine tables. See [Using Multiple Block Devices for Data Storage](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes).

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

#### How to Set Partition Expression {#alter-how-to-specify-part-expr}

You can specify the partition expression in `ALTER ... PARTITION` queries in different ways:

-   As a value from the `partition` column of the `system.parts` table. For example, `ALTER TABLE visits DETACH PARTITION 201901`.
-   As the expression from the table column. Constants and constant expressions are supported. For example, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   Using the partition ID. Partition ID is a string identifier of the partition (human-readable, if possible) that is used as the names of partitions in the file system and in ZooKeeper. The partition ID must be specified in the `PARTITION ID` clause, in a single quotes. For example, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   In the [ALTER ATTACH PART](#alter_attach-partition) and [DROP DETACHED PART](#alter_drop-detached) query, to specify the name of a part, use string literal with a value from the `name` column of the [system.detached\_parts](../../operations/system-tables.md#system_tables-detached_parts) table. For example, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

Usage of quotes when specifying the partition depends on the type of partition expression. For example, for the `String` type, you have to specify its name in quotes (`'`). For the `Date` and `Int*` types no quotes are needed.

For old-style tables, you can specify the partition either as a number `201901` or a string `'201901'`. The syntax for the new-style tables is stricter with types (similar to the parser for the VALUES input format).

All the rules above are also true for the [OPTIMIZE](misc.md#misc_operations-optimize) query. If you need to specify the only partition when optimizing a non-partitioned table, set the expression `PARTITION tuple()`. For example:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

The examples of `ALTER ... PARTITION` queries are demonstrated in the tests [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) and [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### Manipulations with Table TTL {#manipulations-with-table-ttl}

You can change [table TTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) with a request of the following form:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### Synchronicity of ALTER Queries {#synchronicity-of-alter-queries}

For non-replicatable tables, all `ALTER` queries are performed synchronously. For replicatable tables, the query just adds instructions for the appropriate actions to `ZooKeeper`, and the actions themselves are performed as soon as possible. However, the query can wait for these actions to be completed on all the replicas.

For `ALTER ... ATTACH|DETACH|DROP` queries, you can use the `replication_alter_partitions_sync` setting to set up waiting.
Possible values: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### Mutations {#alter-mutations}

Mutations are an ALTER query variant that allows changing or deleting rows in a table. In contrast to standard `UPDATE` and `DELETE` queries that are intended for point data changes, mutations are intended for heavy operations that change a lot of rows in a table. Supported for the `MergeTree` family of table engines including the engines with replication support.

Existing tables are ready for mutations as-is (no conversion necessary), but after the first mutation is applied to a table, its metadata format becomes incompatible with previous server versions and falling back to a previous version becomes impossible.

Currently available commands:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

The `filter_expr` must be of type `UInt8`. The query deletes rows in the table for which this expression takes a non-zero value.

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

The `filter_expr` must be of type `UInt8`. This query updates values of specified columns to the values of corresponding expressions in rows for which the `filter_expr` takes a non-zero value. Values are casted to the column type using the `CAST` operator. Updating columns that are used in the calculation of the primary or the partition key is not supported.

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

The query rebuilds the secondary index `name` in the partition `partition_name`.

One query can contain several commands separated by commas.

For \*MergeTree tables mutations execute by rewriting whole data parts. There is no atomicity - parts are substituted for mutated parts as soon as they are ready and a `SELECT` query that started executing during a mutation will see data from parts that have already been mutated along with data from parts that have not been mutated yet.

Mutations are totally ordered by their creation order and are applied to each part in that order. Mutations are also partially ordered with INSERTs - data that was inserted into the table before the mutation was submitted will be mutated and data that was inserted after that will not be mutated. Note that mutations do not block INSERTs in any way.

A mutation query returns immediately after the mutation entry is added (in case of replicated tables to ZooKeeper, for nonreplicated tables - to the filesystem). The mutation itself executes asynchronously using the system profile settings. To track the progress of mutations you can use the [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) table. A mutation that was successfully submitted will continue to execute even if ClickHouse servers are restarted. There is no way to roll back the mutation once it is submitted, but if the mutation is stuck for some reason it can be cancelled with the [`KILL MUTATION`](misc.md#kill-mutation) query.

Entries for finished mutations are not deleted right away (the number of preserved entries is determined by the `finished_mutations_to_keep` storage engine parameter). Older mutation entries are deleted.

## ALTER USER {#alter-user-statement}

Changes ClickHouse user accounts.

### Syntax {#alter-user-syntax}

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```  

### Description {#alter-user-dscr}

To use `ALTER USER` you must have the [ALTER USER](grant.md#grant-access-management) privilege.

### Examples {#alter-user-examples}

Set assigned roles as default:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

If roles aren't previously assigned to a user, ClickHouse throws an exception.

Set all the assigned roles to default:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

If a role is assigned to a user in the future, it will become default automatically.

Set all the assigned roles to default, excepting `role1` and `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```


## ALTER ROLE {#alter-role-statement}

Changes roles.

### Syntax {#alter-role-syntax}

``` sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```


## ALTER ROW POLICY {#alter-row-policy-statement}

Changes row policy.

### Syntax {#alter-row-policy-syntax}

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```


## ALTER QUOTA {#alter-quota-statement}

Changes quotas.

### Syntax {#alter-quota-syntax}

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```


## ALTER SETTINGS PROFILE {#alter-settings-profile-statement}

Changes settings profiles.

### Syntax {#alter-settings-profile-syntax}

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```


[Original article](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
