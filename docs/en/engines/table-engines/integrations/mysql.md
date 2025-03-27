---
slug: /en/engines/table-engines/integrations/mysql
sidebar_position: 138
sidebar_label: MySQL
---

# MySQL Table Engine

The MySQL engine allows you to perform `SELECT` and `INSERT` queries on data that is stored on a remote MySQL server.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300 ]
;
```

See a detailed description of the [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) query.

The table structure can differ from the original MySQL table structure:

- Column names should be the same as in the original MySQL table, but you can use just some of these columns and in any order.
- Column types may differ from those in the original MySQL table. ClickHouse tries to [cast](../../../engines/database-engines/mysql.md#data_types-support) values to the ClickHouse data types.
- The [external_table_functions_use_nulls](../../../operations/settings/settings.md#external-table-functions-use-nulls) setting defines how to handle Nullable columns. Default value: 1. If 0, the table function does not make Nullable columns and inserts default values instead of nulls. This is also applicable for NULL values inside arrays.

:::note
The MySQL Table Engine is currently not available on the ClickHouse builds for MacOS ([issue](https://github.com/ClickHouse/ClickHouse/issues/21191))
:::

**Engine Parameters**

- `host:port` — MySQL server address.
- `database` — Remote database name.
- `table` — Remote table name.
- `user` — MySQL user.
- `password` — User password.
- `replace_query` — Flag that converts `INSERT INTO` queries to `REPLACE INTO`. If `replace_query=1`, the query is substituted.
- `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` expression that is added to the `INSERT` query.
    Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, where `on_duplicate_clause` is `UPDATE c2 = c2 + 1`. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) to find which `on_duplicate_clause` you can use with the `ON DUPLICATE KEY` clause.
    To specify `on_duplicate_clause` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception.

Arguments also can be passed using [named collections](/docs/en/operations/named-collections.md). In this case `host` and `port` should be specified separately. This approach is recommended for production environment.

Simple `WHERE` clauses such as `=, !=, >, >=, <, <=` are executed on the MySQL server.

The rest of the conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to MySQL finishes.

Supports multiple replicas that must be listed by `|`. For example:

```sql
CREATE TABLE test_replicas (id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

## Usage Example {#usage-example}

Create table in MySQL:

``` text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Create table in ClickHouse using plain arguments:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

Or using [named collections](/docs/en/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL(creds, table='test')
```

Retrieving data from MySQL table:

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## Settings {#mysql-settings}

Default settings are not very efficient, since they do not even reuse connections. These settings allow you to increase the number of queries run by the server per second.

### connection_auto_close {#connection-auto-close}

Allows to automatically close the connection after query execution, i.e. disable connection reuse.

Possible values:

- 1 — Auto-close connection is allowed, so the connection reuse is disabled
- 0 — Auto-close connection is not allowed, so the connection reuse is enabled

Default value: `1`.

### connection_max_tries {#connection-max-tries}

Sets the number of retries for pool with failover.

Possible values:

- Positive integer.
- 0 — There are no retries for pool with failover.

Default value: `3`.

### connection_pool_size {#connection-pool-size}

Size of connection pool (if all connections are in use, the query will wait until some connection will be freed).

Possible values:

- Positive integer.

Default value: `16`.

### connection_wait_timeout {#connection-wait-timeout}

Timeout (in seconds) for waiting for free connection (in case of there is already connection_pool_size active connections), 0 - do not wait.

Possible values:

- Positive integer.

Default value: `5`.

### connect_timeout {#connect-timeout}

Connect timeout (in seconds).

Possible values:

- Positive integer.

Default value: `10`.

### read_write_timeout {#read-write-timeout}

Read/write timeout (in seconds).

Possible values:

- Positive integer.

Default value: `300`.

## See Also {#see-also}

- [The mysql table function](../../../sql-reference/table-functions/mysql.md)
- [Using MySQL as a dictionary source](../../../sql-reference/dictionaries/index.md#dictionary-sources#dicts-external_dicts_dict_sources-mysql)
