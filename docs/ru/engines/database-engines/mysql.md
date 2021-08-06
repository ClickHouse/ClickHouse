# MySQL {#mysql}

Позволяет подключаться к базам данных на удалённом MySQL сервере и выполнять запросы `INSERT` и `SELECT` для обмена данными между ClickHouse и MySQL.

Движок баз данных `MySQL` транслирует запросы при передаче на сервер MySQL, что позволяет выполнять и другие виды запросов, например `SHOW TABLES` или `SHOW CREATE TABLE`.

Не поддерживаемые виды запросов:

-   `RENAME`
-   `CREATE TABLE`
-   `ALTER`

## Создание базы данных {#sozdanie-bazy-dannykh}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
```

**Параметры движка**

-   `host:port` — адрес сервера MySQL.
-   `database` — имя базы данных на удалённом сервере.
-   `user` — пользователь MySQL.
-   `password` — пароль пользователя.

## Поддержка типов данных {#podderzhka-tipov-dannykh}

| MySQL                            | ClickHouse                                             |
|----------------------------------|--------------------------------------------------------|
| UNSIGNED TINYINT                 | [UInt8](../../engines/database-engines/mysql.md)       |
| TINYINT                          | [Int8](../../engines/database-engines/mysql.md)        |
| UNSIGNED SMALLINT                | [UInt16](../../engines/database-engines/mysql.md)      |
| SMALLINT                         | [Int16](../../engines/database-engines/mysql.md)       |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../../engines/database-engines/mysql.md)      |
| INT, MEDIUMINT                   | [Int32](../../engines/database-engines/mysql.md)       |
| UNSIGNED BIGINT                  | [UInt64](../../engines/database-engines/mysql.md)      |
| BIGINT                           | [Int64](../../engines/database-engines/mysql.md)       |
| FLOAT                            | [Float32](../../engines/database-engines/mysql.md)     |
| DOUBLE                           | [Float64](../../engines/database-engines/mysql.md)     |
| DATE                             | [Date](../../engines/database-engines/mysql.md)        |
| DATETIME, TIMESTAMP              | [DateTime](../../engines/database-engines/mysql.md)    |
| BINARY                           | [FixedString](../../engines/database-engines/mysql.md) |

Все прочие типы данных преобразуются в [String](../../engines/database-engines/mysql.md).

[Nullable](../../engines/database-engines/mysql.md) поддержан.

## Примеры использования {#primery-ispolzovaniia}

Таблица в MySQL:

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
+--------+-------+
| int_id | value |
+--------+-------+
|      1 |     2 |
+--------+-------+
1 row in set (0,00 sec)
```

База данных в ClickHouse, позволяющая обмениваться данными с сервером MySQL:

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
