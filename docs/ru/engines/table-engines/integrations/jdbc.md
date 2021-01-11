# JDBC {#table-engine-jdbc}

Позволяет ClickHouse подключаться к внешним базам данных с помощью [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

Для реализации соединения по JDBC ClickHouse использует отдельную программу [clickhouse-jdbc-bridge](https://github.com/alex-krash/clickhouse-jdbc-bridge), которая должна запускаться как демон.

Движок поддерживает тип данных [Nullable](../../../engines/table-engines/integrations/jdbc.md).

## Создание таблицы {#sozdanie-tablitsy}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
ENGINE = JDBC(dbms_uri, external_database, external_table)
```

**Параметры движка**

-   `dbms_uri` — URI внешней СУБД.

        Формат: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.

    Пример для MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — база данных во внешней СУБД.

-   `external_table` — таблица в `external_database`.

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

## Смотрите также {#smotrite-takzhe}

-   [Табличная функция JDBC](../../../engines/table-engines/integrations/jdbc.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/jdbc/) <!--hide-->
