# JDBC

Allows ClickHouse to connect to external databases via [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

To implement JDBC connection, ClickHouse uses the separate program [clickhouse-jdbc-bridge](https://github.com/alex-krash/clickhouse-jdbc-bridge). You should run it as a daemon.

This engine supports the [Nullable](../../data_types/nullable.md) data type.

## Creating a Table

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name  ENGINE = JDBC(dbms_uri, external_database, external_table)
```

**Engine Parameters**

- `dbms_uri` — URI of an external DBMS.

    Format: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    Example for MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

- `external_database` — Database in an external DBMS.
- `external_table` — A name of the table in `external_database`.

## Usage Example

Creating a table in MySQL (using native MySQL engine):

```
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
+--------+--------------+-------+----------------+
| int_id | int_nullable | float | float_nullable |
+--------+--------------+-------+----------------+
|      1 |         NULL |     2 |           NULL |
+--------+--------------+-------+----------------+
1 row in set (0,00 sec)
```

Selecting data from the table in ClickHouse:

```
CREATE TABLE jdbc_table ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')

Ok.

DESCRIBE TABLE jdbc_table

┌─name───────────────┬─type───────────────┬─default_type─┬─default_expression─┐
│ int_id             │ Int32              │              │                    │
│ int_nullable       │ Nullable(Int32)    │              │                    │
│ float              │ Float32            │              │                    │
│ float_nullable     │ Nullable(Float32)  │              │                    │
└────────────────────┴────────────────────┴──────────────┴────────────────────┘

10 rows in set. Elapsed: 0.031 sec.

SELECT *
FROM jdbc_table

┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘

1 rows in set. Elapsed: 0.055 sec.
```

## See Also

- [JDBC table function](../../query_language/table_functions/jdbc.md).
