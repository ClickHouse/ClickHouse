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

### Default Values

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

### Temporary Tables

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

