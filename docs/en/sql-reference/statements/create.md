---
toc_priority: 35
toc_title: CREATE
---

# CREATE Queries {#create-queries}

## CREATE DATABASE {#query-language-create-database}

Creates database.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### Clauses {#clauses}

-   `IF NOT EXISTS`
    If the `db_name` database already exists, then ClickHouse doesn’t create a new database and:

    -   Doesn’t throw an exception if clause is specified.
    -   Throws an exception if clause isn’t specified.

-   `ON CLUSTER`
    ClickHouse creates the `db_name` database on all the servers of a specified cluster.

-   `ENGINE`

    -   [MySQL](../../engines/database-engines/mysql.md)
        Allows you to retrieve data from the remote MySQL server.
        By default, ClickHouse uses its own [database engine](../../engines/database-engines/index.md).

## CREATE TABLE {#create-table-query}

The `CREATE TABLE` query can have several forms.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
```

Creates a table named ‘name’ in the ‘db’ database or the current database if ‘db’ is not set, with the structure specified in brackets and the ‘engine’ engine.
The structure of the table is a list of column descriptions. If indexes are supported by the engine, they are indicated as parameters for the table engine.

A column description is `name type` in the simplest case. Example: `RegionID UInt32`.
Expressions can also be defined for default values (see below).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

Creates a table with the same structure as another table. You can specify a different engine for the table. If the engine is not specified, the same engine will be used as for the `db2.name2` table.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Creates a table with the structure and data returned by a [table function](../table-functions/index.md#table-functions).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

Creates a table with a structure like the result of the `SELECT` query, with the ‘engine’ engine, and fills it with data from SELECT.

In all cases, if `IF NOT EXISTS` is specified, the query won’t return an error if the table already exists. In this case, the query won’t do anything.

There can be other clauses after the `ENGINE` clause in the query. See detailed documentation on how to create tables in the descriptions of [table engines](../../engines/table-engines/index.md#table_engines).

### Default Values {#create-default-values}

The column description can specify an expression for a default value, in one of the following ways:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
Example: `URLDomain String DEFAULT domain(URL)`.

If an expression for the default value is not defined, the default values will be set to zeros for numbers, empty strings for strings, empty arrays for arrays, and `0000-00-00` for dates or `0000-00-00 00:00:00` for dates with time. NULLs are not supported.

If the default expression is defined, the column type is optional. If there isn’t an explicitly defined type, the default expression type is used. Example: `EventDate DEFAULT toDate(EventTime)` – the ‘Date’ type will be used for the ‘EventDate’ column.

If the data type and default expression are defined explicitly, this expression will be cast to the specified type using type casting functions. Example: `Hits UInt32 DEFAULT 0` means the same thing as `Hits UInt32 DEFAULT toUInt32(0)`.

Default expressions may be defined as an arbitrary expression from table constants and columns. When creating and changing the table structure, it checks that expressions don’t contain loops. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

`DEFAULT expr`

Normal default value. If the INSERT query doesn’t specify the corresponding column, it will be filled in by computing the corresponding expression.

`MATERIALIZED expr`

Materialized expression. Such a column can’t be specified for INSERT, because it is always calculated.
For an INSERT without a list of columns, these columns are not considered.
In addition, this column is not substituted when using an asterisk in a SELECT query. This is to preserve the invariant that the dump obtained using `SELECT *` can be inserted back into the table using INSERT without specifying the list of columns.

`ALIAS expr`

Synonym. Such a column isn’t stored in the table at all.
Its values can’t be inserted in a table, and it is not substituted when using an asterisk in a SELECT query.
It can be used in SELECTs if the alias is expanded during query parsing.

When using the ALTER query to add new columns, old data for these columns is not written. Instead, when reading old data that does not have values for the new columns, expressions are computed on the fly by default. However, if running the expressions requires different columns that are not indicated in the query, these columns will additionally be read, but only for the blocks of data that need it.

If you add a new column to a table but later change its default expression, the values used for old data will change (for data where values were not stored on the disk). Note that when running background merges, data for columns that are missing in one of the merging parts is written to the merged part.

It is not possible to set default values for elements in nested data structures.

### Constraints {#constraints}

Along with columns descriptions constraints could be defined:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` could by any boolean expression. If constraints are defined for the table, each of them will be checked for every row in `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

Adding large amount of constraints can negatively affect performance of big `INSERT` queries.

### TTL Expression {#ttl-expression}

Defines storage time for values. Can be specified only for MergeTree-family tables. For the detailed description, see [TTL for columns and tables](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

### Column Compression Codecs {#codecs}

By default, ClickHouse applies the `lz4` compression method. For `MergeTree`-engine family you can change the default compression method in the [compression](../../operations/server-configuration-parameters/settings.md#server-settings-compression) section of a server configuration. You can also define the compression method for each individual column in the `CREATE TABLE` query.

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

If a codec is specified, the default codec doesn’t apply. Codecs can be combined in a pipeline, for example, `CODEC(Delta, ZSTD)`. To select the best codec combination for you project, pass benchmarks similar to described in the Altinity [New Encodings to Improve ClickHouse Efficiency](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse) article.

!!! warning "Warning"
    You can’t decompress ClickHouse database files with external utilities like `lz4`. Instead, use the special [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) utility.

Compression is supported for the following table engines:

-   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family. Supports column compression codecs and selecting the default compression method by [compression](../../operations/server-configuration-parameters/settings.md#server-settings-compression) settings.
-   [Log](../../engines/table-engines/log-family/log-family.md) family. Uses the `lz4` compression method by default and supports column compression codecs.
-   [Set](../../engines/table-engines/special/set.md). Only supported the default compression.
-   [Join](../../engines/table-engines/special/join.md). Only supported the default compression.

ClickHouse supports common purpose codecs and specialized codecs.

#### Specialized Codecs {#create-query-specialized-codecs}

These codecs are designed to make compression more effective by using specific features of data. Some of these codecs don’t compress data themself. Instead, they prepare the data for a common purpose codec, which compresses it better than without this preparation.

Specialized codecs:

-   `Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` are used for storing delta values, so `delta_bytes` is the maximum size of raw values. Possible `delta_bytes` values: 1, 2, 4, 8. The default value for `delta_bytes` is `sizeof(type)` if equal to 1, 2, 4, or 8. In all other cases, it’s 1.
-   `DoubleDelta` — Calculates delta of deltas and writes it in compact binary form. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-byte deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` — Calculates XOR between current and previous value and writes it in compact binary form. Efficient when storing a series of floating point values that change slowly, because the best compression rate is achieved when neighboring values are binary equal. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. For additional information, see Compressing Values in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` and `DateTime`). At each step of its algorithm, codec takes a block of 64 values, puts them into 64x64 bit matrix, transposes it, crops the unused bits of values and returns the rest as a sequence. Unused bits are the bits, that don’t differ between maximum and minimum values in the whole data part for which the compression is used.

`DoubleDelta` and `Gorilla` codecs are used in Gorilla TSDB as the components of its compressing algorithm. Gorilla approach is effective in scenarios when there is a sequence of slowly changing values with their timestamps. Timestamps are effectively compressed by the `DoubleDelta` codec, and values are effectively compressed by the `Gorilla` codec. For example, to get an effectively stored table, you can create it in the following configuration:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### General Purpose Codecs {#create-query-general-purpose-codecs}

Codecs:

-   `NONE` — No compression.
-   `LZ4` — Lossless [data compression algorithm](https://github.com/lz4/lz4) used by default. Applies LZ4 fast compression.
-   `LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` applies the default level. Possible levels: \[1, 12\]. Recommended level range: \[4, 9\].
-   `ZSTD[(level)]` — [ZSTD compression algorithm](https://en.wikipedia.org/wiki/Zstandard) with configurable `level`. Possible levels: \[1, 22\]. Default value: 1.

High compression levels are useful for asymmetric scenarios, like compress once, decompress repeatedly. Higher levels mean better compression and higher CPU usage.

## Temporary Tables {#temporary-tables}

ClickHouse supports temporary tables which have the following characteristics:

-   Temporary tables disappear when the session ends, including if the connection is lost.
-   A temporary table uses the Memory engine only.
-   The DB can’t be specified for a temporary table. It is created outside of databases.
-   Impossible to create a temporary table with distributed DDL query on all cluster servers (by using `ON CLUSTER`): this table exists only in the current session.
-   If a temporary table has the same name as another one and a query specifies the table name without specifying the DB, the temporary table will be used.
-   For distributed query processing, temporary tables used in a query are passed to remote servers.

To create a temporary table, use the following syntax:

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
```

In most cases, temporary tables are not created manually, but when using external data for a query, or for distributed `(GLOBAL) IN`. For more information, see the appropriate sections

It’s possible to use tables with [ENGINE = Memory](../../engines/table-engines/special/memory.md) instead of temporary tables.

## Distributed DDL Queries (ON CLUSTER Clause) {#distributed-ddl-queries-on-cluster-clause}

The `CREATE`, `DROP`, `ALTER`, and `RENAME` queries support distributed execution on a cluster.
For example, the following query creates the `all_hits` `Distributed` table on each host in `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

In order to run these queries correctly, each host must have the same cluster definition (to simplify syncing configs, you can use substitutions from ZooKeeper). They must also connect to the ZooKeeper servers.
The local version of the query will eventually be implemented on each host in the cluster, even if some hosts are currently not available. The order for executing queries within a single host is guaranteed.

## CREATE VIEW {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Creates a view. There are two types of views: normal and MATERIALIZED.

Normal views don’t store any data, but just perform a read from another table. In other words, a normal view is nothing more than a saved query. When reading from a view, this saved query is used as a subquery in the FROM clause.

As an example, assume you’ve created a view:

``` sql
CREATE VIEW view AS SELECT ...
```

and written a query:

``` sql
SELECT a, b, c FROM view
```

This query is fully equivalent to using the subquery:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

Materialized views store data transformed by the corresponding SELECT query.

When creating a materialized view without `TO [db].[table]`, you must specify ENGINE – the table engine for storing data.

When creating a materialized view with `TO [db].[table]`, you must not use `POPULATE`.

A materialized view is arranged as follows: when inserting data to the table specified in SELECT, part of the inserted data is converted by this SELECT query, and the result is inserted in the view.

If you specify POPULATE, the existing table data is inserted in the view when creating it, as if making a `CREATE TABLE ... AS SELECT ...` . Otherwise, the query contains only the data inserted in the table after creating the view. We don’t recommend using POPULATE, since data inserted in the table during the view creation will not be inserted in it.

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won’t be further aggregated. The exception is when using an ENGINE that independently performs data aggregation, such as `SummingMergeTree`.

The execution of `ALTER` queries on materialized views has not been fully developed, so they might be inconvenient. If the materialized view uses the construction `TO [db.]name`, you can `DETACH` the view, run `ALTER` for the target table, and then `ATTACH` the previously detached (`DETACH`) view.

Views look the same as normal tables. For example, they are listed in the result of the `SHOW TABLES` query.

There isn’t a separate query for deleting views. To delete a view, use `DROP TABLE`.

## CREATE DICTIONARY {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME([MIN val1] MAX val2)
```

Creates [external dictionary](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) with given [structure](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [source](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [layout](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) and [lifetime](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

External dictionary structure consists of attributes. Dictionary attributes are specified similarly to table columns. The only required attribute property is its type, all other properties may have default values.

Depending on dictionary [layout](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) one or more attributes can be specified as dictionary keys.

For more information, see [External Dictionaries](../dictionaries/external-dictionaries/external-dicts.md) section.

## CREATE USER {#create-user-statement}

Creates a [user account](../../operations/access-rights.md#user-account-management).

### Syntax {#create-user-syntax}

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

#### Identification

There are multiple ways of user identification:

- `IDENTIFIED WITH no_password`
- `IDENTIFIED WITH plaintext_password BY 'qwerty'`
- `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
- `IDENTIFIED WITH sha256_hash BY 'hash'`
- `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
- `IDENTIFIED WITH double_sha1_hash BY 'hash'`

#### User Host

User host is a host from which a connection to ClickHouse server could be established. Host can be specified in the `HOST` section of query by the following ways:

- `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [subnetwork](https://en.wikipedia.org/wiki/Subnetwork). Examples: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. For use in production, only specify `HOST IP` elements (IP addresses and their masks), since using `host` and `host_regexp` might cause extra latency.
- `HOST ANY` — User can connect from any location. This is default option.
- `HOST LOCAL` — User can connect only locally.
- `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
- `HOST NAME REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) regular expressions when specifying user hosts. For example, `HOST NAME REGEXP '.*\.mysite\.com'`.
- `HOST LIKE 'template'` — Allows you use the [LIKE](../functions/string-search-functions.md#function-like) operator to filter the user hosts. For example, `HOST LIKE '%'` is equivalent to `HOST ANY`, `HOST LIKE '%.mysite.com'` filters all the hosts in the `mysite.com` domain.

Another way of specifying host is to use `@` syntax with the user name. Examples:

- `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` syntax.
- `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` syntax.
- `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` syntax.

!!! info "Warning"
    ClickHouse treats `user_name@'address'` as a user name as a whole. Thus, technically you can create multiple users with `user_name` and different constructions after `@`. We don't recommend to do so.


### Examples {#create-user-examples}


Create the user account `mira` protected by the password `qwerty`:

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty'
```

`mira` should start client app at the host where the ClickHouse server runs.

Create the user account `john`, assign roles to it and make this roles default:

``` sql
CREATE USER john DEFAULT ROLE role1, role2
```

Create the user account `john` and make all his future roles default:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

When some role will be assigned to `john` in the future it will become default automatically.

Create the user account `john` and make all his future roles default excepting `role1` and `role2`:

``` sql
ALTER USER john DEFAULT ROLE ALL EXCEPT role1, role2
```


## CREATE ROLE {#create-role-statement}

Creates a [role](../../operations/access-rights.md#role-management).

### Syntax {#create-role-syntax}

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### Description {#create-role-description}

Role is a set of [privileges](grant.md#grant-privileges). A user granted with a role gets all the privileges of this role. 

A user can be assigned with multiple roles. Users can apply their granted roles in arbitrary combinations by the [SET ROLE](misc.md#set-role-statement) statement. The final scope of privileges is a combined set of all the privileges of all the applied roles. If a user has privileges granted directly to it's user account, they are also combined with the privileges granted by roles.

User can have default roles which apply at user login. To set default roles, use the [SET DEFAULT ROLE](misc.md#set-default-role-statement) statement or the [ALTER USER](alter.md#alter-user-statement) statement.

To revoke a role, use the [REVOKE](revoke.md) statement.

To delete role, use the [DROP ROLE](misc.md#drop-role-statement) statement. The deleted role is being automatically revoked from all the users and roles to which it was granted.

### Examples {#create-role-examples}

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

This sequence of queries creates the role `accountant` that has the privilege of reading data from the `accounting` database.

Granting the role to the user `mira`:

```sql
GRANT accountant TO mira;
```

After the role is granted, the user can use it and perform the allowed queries. For example:

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```

## CREATE ROW POLICY {#create-row-policy-statement}

Creates a [filter for rows](../../operations/access-rights.md#row-policy-management), which a user can read from a table.

### Syntax {#create-row-policy-syntax}

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

#### Section AS {#create-row-policy-as}

Using this section you can create permissive or restrictive policies.

Permissive policy grants access to rows. Permissive policies which apply to the same table are combined together using the boolean `OR` operator. Policies are permissive by default.

Restrictive policy restricts access to row. Restrictive policies which apply to the same table are combined together using the boolean `AND` operator.

Restrictive policies apply to rows that passed the permissive filters. If you set restrictive policies but no permissive policies, the user can't get any row from the table.

#### Section TO {#create-row-policy-to}

In the section `TO` you can give a mixed list of roles and users, for example, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Keyword `ALL` means all the ClickHouse users including current user. Keywords `ALL EXCEPT` allow to to exclude some users from the all users list, for example `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

### Examples

- `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO accountant, john@localhost`
- `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO ALL EXCEPT mira`


## CREATE QUOTA {#create-quota-statement}

Creates a [quota](../../operations/access-rights.md#quota-management) that can be assigned to a user or a role.

### Syntax {#create-quota-syntax}

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

### Example {#create-quota-example}

Limit the maximum number of queries for the current user with 123 queries in 15 months constraint:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER
```


## CREATE SETTINGS PROFILE {#create-settings-profile-statement}

Creates a [settings profile](../../operations/access-rights.md#settings-profile-management) that can be assigned to a user or a role.

### Syntax {#create-settings-profile-syntax}

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

# Example {#create-settings-profile-syntax}

Create the `max_memory_usage_profile` settings profile with value and constraints for the `max_memory_usage` setting. Assign it to `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```


[Original article](https://clickhouse.tech/docs/en/query_language/create/) <!--hide-->
