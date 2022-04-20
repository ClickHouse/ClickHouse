---
sidebar_position: 3
sidebar_label: JDBC
---

# JDBC {#table-engine-jdbc}

Позволяет ClickHouse подключаться к внешним базам данных с помощью [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

Для реализации соединения по JDBC ClickHouse использует отдельную программу [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge), которая должна запускаться как демон.

Движок поддерживает тип данных [Nullable](../../../engines/table-engines/integrations/jdbc.md).

## Создание таблицы {#sozdanie-tablitsy}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
ENGINE = JDBC(datasource_uri, external_database, external_table)
```

**Параметры движка**

-   `datasource_uri` — URI или имя внешней СУБД.

    URI Формат: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.

    Пример для MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — база данных во внешней СУБД.

-   `external_table` — таблицы в `external_database` или запросе выбора, например` select * from table1, где column1 = 1`.

## Пример использования {#primer-ispolzovaniia}

Создадим таблицу в на сервере MySQL с помощью консольного клиента MySQL:

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
+--------+--------------+-------+----------------+
| int_id | int_nullable | float | float_nullable |
+--------+--------------+-------+----------------+
|      1 |         NULL |     2 |           NULL |
+--------+--------------+-------+----------------+
1 row in set (0,00 sec)
```

Создадим таблицу на сервере ClickHouse и получим из неё данные:

``` sql
CREATE TABLE jdbc_table ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

``` sql
DESCRIBE TABLE jdbc_table
```

``` text
┌─name───────────────┬─type───────────────┬─default_type─┬─default_expression─┐
│ int_id             │ Int32              │              │                    │
│ int_nullable       │ Nullable(Int32)    │              │                    │
│ float              │ Float32            │              │                    │
│ float_nullable     │ Nullable(Float32)  │              │                    │
└────────────────────┴────────────────────┴──────────────┴────────────────────┘
```

``` sql
SELECT *
FROM jdbc_table
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

``` sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

## Смотрите также {#smotrite-takzhe}

-   [Табличная функция JDBC](../../../engines/table-engines/integrations/jdbc.md).

