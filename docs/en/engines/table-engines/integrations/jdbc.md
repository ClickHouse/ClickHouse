---
description: 'Allows ClickHouse to connect to external databases via JDBC.'
sidebar_label: 'JDBC'
sidebar_position: 100
slug: /engines/table-engines/integrations/jdbc
title: 'JDBC table engine'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# JDBC table engine

<CloudNotSupportedBadge/>

:::note
clickhouse-jdbc-bridge contains experimental codes and is no longer supported. It may contain reliability issues and security vulnerabilities. Use it at your own risk. 
ClickHouse recommend using built-in table functions in ClickHouse which provide a better alternative for ad-hoc querying scenarios (Postgres, MySQL, MongoDB, etc).
:::

Allows ClickHouse to connect to external databases via [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

To implement the JDBC connection, ClickHouse uses the separate program [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) that should run as a daemon.

This engine supports the [Nullable](../../../sql-reference/data-types/nullable.md) data type.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource, external_database, external_table)
```

**Engine Parameters**

- `datasource` — URI or name of an external DBMS.

    URI Format: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    Example for MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

- `external_database` — Name of a database in an external DBMS, or, instead, an explicitly defined table schema (see examples).

- `external_table` — Name of the table in an external database or a select query like `select * from table1 where column1=1`.

- These parameters can also be passed using [named collections](operations/named-collections.md).

## Usage example {#usage-example}

Creating a table in MySQL server by connecting directly with it's console client:

```text
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

Creating a table in ClickHouse server and selecting data from it:

```sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

```sql
SELECT *
FROM jdbc_table
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

```sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

## See also {#see-also}

- [JDBC table function](../../../sql-reference/table-functions/jdbc.md).
