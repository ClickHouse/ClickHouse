---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 30
toc_title: MySQL
---

# Mysql {#mysql}

Permite conectarse a bases de datos en un servidor MySQL remoto y realizar `INSERT` y `SELECT` consultas para intercambiar datos entre ClickHouse y MySQL.

El `MySQL` motor de base de datos traducir consultas al servidor MySQL para que pueda realizar operaciones tales como `SHOW TABLES` o `SHOW CREATE TABLE`.

No puede realizar las siguientes consultas:

-   `RENAME`
-   `CREATE TABLE`
-   `ALTER`

## Creación de una base de datos {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', 'database', 'user', 'password')
```

**Parámetros del motor**

-   `host:port` — MySQL server address.
-   `database` — Remote database name.
-   `user` — MySQL user.
-   `password` — User password.

## Soporte de tipos de datos {#data_types-support}

| MySQL                            | Haga clic en Casa                                            |
|----------------------------------|--------------------------------------------------------------|
| UNSIGNED TINYINT                 | [UInt8](../../sql_reference/data_types/int_uint.md)          |
| TINYINT                          | [Int8](../../sql_reference/data_types/int_uint.md)           |
| UNSIGNED SMALLINT                | [UInt16](../../sql_reference/data_types/int_uint.md)         |
| SMALLINT                         | [Int16](../../sql_reference/data_types/int_uint.md)          |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../../sql_reference/data_types/int_uint.md)         |
| INT, MEDIUMINT                   | [Int32](../../sql_reference/data_types/int_uint.md)          |
| UNSIGNED BIGINT                  | [UInt64](../../sql_reference/data_types/int_uint.md)         |
| BIGINT                           | [Int64](../../sql_reference/data_types/int_uint.md)          |
| FLOAT                            | [Float32](../../sql_reference/data_types/float.md)           |
| DOUBLE                           | [Float64](../../sql_reference/data_types/float.md)           |
| DATE                             | [Fecha](../../sql_reference/data_types/date.md)              |
| DATETIME, TIMESTAMP              | [FechaHora](../../sql_reference/data_types/datetime.md)      |
| BINARY                           | [Cadena fija](../../sql_reference/data_types/fixedstring.md) |

Todos los demás tipos de datos MySQL se convierten en [Cadena](../../sql_reference/data_types/string.md).

[NULL](../../sql_reference/data_types/nullable.md) se admite.

## Ejemplos de uso {#examples-of-use}

Tabla en MySQL:

``` text
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+------+-----+
| int_id | value |
+------+-----+
|      1 |     2 |
+------+-----+
1 row in set (0,00 sec)
```

Base de datos en ClickHouse, intercambiando datos con el servidor MySQL:

``` sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
```

``` sql
SHOW DATABASES
```

``` text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

``` sql
SHOW TABLES FROM mysql_db
```

``` text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

``` sql
SELECT * FROM mysql_db.mysql_table
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

``` sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

``` sql
SELECT * FROM mysql_db.mysql_table
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/database_engines/mysql/) <!--hide-->
