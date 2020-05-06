---
toc_priority: 41
toc_title: Other
---

# Miscellaneous Queries {#miscellaneous-queries}

## ATTACH {#attach}

This query is exactly the same as `CREATE`, but

-   Instead of the word `CREATE` it uses the word `ATTACH`.
-   The query does not create data on the disk, but assumes that data is already in the appropriate places, and just adds information about the table to the server.
    After executing an ATTACH query, the server will know about the existence of the table.

If the table was previously detached (`DETACH`), meaning that its structure is known, you can use shorthand without defining the structure.

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

This query is used when starting the server. The server stores table metadata as files with `ATTACH` queries, which it simply runs at launch (with the exception of system tables, which are explicitly created on the server).

## CHECK TABLE {#check-table}

Checks if the data in the table is corrupted.

``` sql
CHECK TABLE [db.]name
```

The `CHECK TABLE` query compares actual file sizes with the expected values which are stored on the server. If the file sizes do not match the stored values, it means the data is corrupted. This can be caused, for example, by a system crash during query execution.

The query response contains the `result` column with a single row. The row has a value of
[Boolean](../../sql-reference/data-types/boolean.md) type:

-   0 - The data in the table is corrupted.
-   1 - The data maintains integrity.

The `CHECK TABLE` query supports the following table engines:

-   [Log](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md)
-   [MergeTree family](../../engines/table-engines/mergetree-family/mergetree.md)

Performed over the tables with another table engines causes an exception.

Engines from the `*Log` family don’t provide automatic data recovery on failure. Use the `CHECK TABLE` query to track data loss in a timely manner.

For `MergeTree` family engines, the `CHECK TABLE` query shows a check status for every individual data part of a table on the local server.

**If the data is corrupted**

If the table is corrupted, you can copy the non-corrupted data to another table. To do this:

1.  Create a new table with the same structure as damaged table. To do this execute the query `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  Set the [max\_threads](../../operations/settings/settings.md#settings-max_threads) value to 1 to process the next query in a single thread. To do this run the query `SET max_threads = 1`.
3.  Execute the query `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. This request copies the non-corrupted data from the damaged table to another table. Only the data before the corrupted part will be copied.
4.  Restart the `clickhouse-client` to reset the `max_threads` value.

## DESCRIBE TABLE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Returns the following `String` type columns:

-   `name` — Column name.
-   `type`— Column type.
-   `default_type` — Clause that is used in [default expression](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` or `ALIAS`). Column contains an empty string, if the default expression isn’t specified.
-   `default_expression` — Value specified in the `DEFAULT` clause.
-   `comment_expression` — Comment text.

Nested data structures are output in “expanded” format. Each column is shown separately, with the name after a dot.

## DETACH {#detach-statement}

Deletes information about the ‘name’ table from the server. The server stops knowing about the table’s existence.

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

This does not delete the table’s data or metadata. On the next server launch, the server will read the metadata and find out about the table again.
Similarly, a “detached” table can be re-attached using the `ATTACH` query (with the exception of system tables, which do not have metadata stored for them).

There is no `DETACH DATABASE` query.

## DROP {#drop}

This query has two types: `DROP DATABASE` and `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

Deletes all tables inside the ‘db’ database, then deletes the ‘db’ database itself.
If `IF EXISTS` is specified, it doesn’t return an error if the database doesn’t exist.

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Deletes the table.
If `IF EXISTS` is specified, it doesn’t return an error if the table doesn’t exist or the database doesn’t exist.

    DROP DICTIONARY [IF EXISTS] [db.]name

Delets the dictionary.
If `IF EXISTS` is specified, it doesn’t return an error if the table doesn’t exist or the database doesn’t exist.

## DROP USER {#drop-user-statement}

Deletes a user.

### Syntax {#drop-user-syntax}

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```


## DROP ROLE {#drop-role-statement}

Deletes a role.

Deleted role is revoked from all the entities where it was granted.

### Syntax {#drop-role-syntax}

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

Deletes a row policy.

Deleted row policy is revoked from all the entities where it was assigned.

### Syntax {#drop-row-policy-syntax}

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```


## DROP QUOTA {#drop-quota-statement}

Deletes a quota.

Deleted quota is revoked from all the entities where it was assigned.

### Syntax {#drop-quota-syntax}

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```


## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

Deletes a quota.

Deleted quota is revoked from all the entities where it was assigned.

### Syntax {#drop-settings-profile-syntax}

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```


## EXISTS {#exists-statement}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Returns a single `UInt8`-type column, which contains the single value `0` if the table or database doesn’t exist, or `1` if the table exists in the specified database.

## KILL QUERY {#kill-query-statement}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Attempts to forcibly terminate the currently running queries.
The queries to terminate are selected from the system.processes table using the criteria defined in the `WHERE` clause of the `KILL` query.

Examples:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

Read-only users can only stop their own queries.

By default, the asynchronous version of queries is used (`ASYNC`), which doesn’t wait for confirmation that queries have stopped.

The synchronous version (`SYNC`) waits for all queries to stop and displays information about each process as it stops.
The response contains the `kill_status` column, which can take the following values:

1.  ‘finished’ – The query was terminated successfully.
2.  ‘waiting’ – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can’t be stopped.

A test query (`TEST`) only checks the user’s rights and displays a list of queries to stop.

## KILL MUTATION {#kill-mutation-statement}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Tries to cancel and remove [mutations](alter.md#alter-mutations) that are currently executing. Mutations to cancel are selected from the [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) table using the filter specified by the `WHERE` clause of the `KILL` query.

A test query (`TEST`) only checks the user’s rights and displays a list of queries to stop.

Examples:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

Changes already made by the mutation are not rolled back.

## OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

This query tries to initialize an unscheduled merge of data parts for tables with a table engine from the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family.

The `OPTMIZE` query is also supported for the [MaterializedView](../../engines/table-engines/special/materializedview.md) and the [Buffer](../../engines/table-engines/special/buffer.md) engines. Other table engines aren’t supported.

When `OPTIMIZE` is used with the [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) family of table engines, ClickHouse creates a task for merging and waits for execution on all nodes (if the `replication_alter_partitions_sync` setting is enabled).

-   If `OPTIMIZE` doesn’t perform a merge for any reason, it doesn’t notify the client. To enable notifications, use the [optimize\_throw\_if\_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) setting.
-   If you specify a `PARTITION`, only the specified partition is optimized. [How to set partition expression](alter.md#alter-how-to-specify-part-expr).
-   If you specify `FINAL`, optimization is performed even when all the data is already in one part.
-   If you specify `DEDUPLICATE`, then completely identical rows will be deduplicated (all columns are compared), it makes sense only for the MergeTree engine.

!!! warning "Warning"
    `OPTIMIZE` can’t fix the “Too many parts” error.

## RENAME {#misc_operations-rename}

Renames one or more tables.

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

All tables are renamed under global locking. Renaming tables is a light operation. If you indicated another database after TO, the table will be moved to this database. However, the directories with databases must reside in the same file system (otherwise, an error is returned).

## SET {#query-set}

``` sql
SET param = value
```

Assigns `value` to the `param` [setting](../../operations/settings/index.md) for the current session. You cannot change [server settings](../../operations/server-configuration-parameters/index.md) this way.

You can also set all the values from the specified settings profile in a single query.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

For more information, see [Settings](../../operations/settings/settings.md).

## SET ROLE {#set-role-statement}

Activates roles for the current user.

### Syntax {#set-role-syntax}

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

Sets default roles to a user.

Default roles are automatically activated at user login. You can set as default only the previously granted roles. If the role isn't granted to a user, ClickHouse throws an exception.


### Syntax {#set-default-role-syntax}

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```


### Examples {#set-default-role-examples}

Set multiple default roles to a user:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Set all the granted roles as default to a user:

``` sql
SET DEFAULT ROLE ALL TO user
```

Purge default roles from a user:

``` sql
SET DEFAULT ROLE NONE TO user
```

Set all the granted roles as default excepting some of them:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```


## TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Removes all data from a table. When the clause `IF EXISTS` is omitted, the query returns an error if the table does not exist.

The `TRUNCATE` query is not supported for [View](../../engines/table-engines/special/view.md), [File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) and [Null](../../engines/table-engines/special/null.md) table engines.

## USE {#use}

``` sql
USE db
```

Lets you set the current database for the session.
The current database is used for searching for tables if the database is not explicitly defined in the query with a dot before the table name.
This query can’t be made when using the HTTP protocol, since there is no concept of a session.

[Original article](https://clickhouse.tech/docs/en/query_language/misc/) <!--hide-->
