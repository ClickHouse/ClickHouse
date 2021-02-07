---
toc_priority: 1
toc_title: ODBC
---

# ODBC {#table-engine-odbc}

Позволяет ClickHouse подключаться к внешним базам данных с помощью [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

Чтобы использование ODBC было безопасным, ClickHouse использует отдельную программу `clickhouse-odbc-bridge`. Если драйвер ODBC подгружать непосредственно из `clickhouse-server`, то проблемы с драйвером могут привести к аварийной остановке сервера ClickHouse. ClickHouse автоматически запускает `clickhouse-odbc-bridge` по мере необходимости. Программа устанавливается из того же пакета, что и `clickhouse-server`.

Движок поддерживает тип данных [Nullable](../../../engines/table-engines/integrations/odbc.md).

## Создание таблицы {#sozdanie-tablitsy}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(connection_settings, external_database, external_table)
```

Смотрите подробное описание запроса [CREATE TABLE](../../../engines/table-engines/integrations/odbc.md#create-table-query).

Структура таблицы может отличаться от структуры исходной таблицы в удалённой СУБД:

-   Имена столбцов должны быть такими же, как в исходной таблице, но вы можете использовать только некоторые из этих столбцов и в любом порядке.
-   Типы столбцов могут отличаться от типов аналогичных столбцов в исходной таблице. ClickHouse пытается [приводить](../../../engines/table-engines/integrations/odbc.md#type_conversion_function-cast) значения к типам данных ClickHouse.

**Параметры движка**

-   `connection_settings` — название секции с настройками соединения в файле `odbc.ini`.
-   `external_database` — имя базы данных во внешней СУБД.
-   `external_table` — имя таблицы в `external_database`.

## Пример использования {#primer-ispolzovaniia}

**Извлечение данных из локальной установки MySQL через ODBC**

Этот пример проверялся в Ubuntu Linux 18.04 для MySQL server 5.7.

Убедитесь, что unixODBC и MySQL Connector установлены.

По умолчанию (если установлен из пакетов) ClickHouse запускается от имени пользователя `clickhouse`. Таким образом, вам нужно создать и настроить этого пользователя на сервере MySQL.

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

Теперь настроим соединение в `/etc/odbc.ini`.

``` bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

Вы можете проверить соединение с помощью утилиты `isql` из установки unixODBC.

``` bash
$ isql -v mysqlconn
+---------------------------------------+
| Connected!                            |
|                                       |
...
```

Таблица в MySQL:

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

Таблица в ClickHouse, которая получает данные из таблицы MySQL:

``` sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

``` sql
SELECT * FROM odbc_t
```

``` text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

## Смотрите также {#smotrite-takzhe}

-   [Внешние словари ODBC](../../../engines/table-engines/integrations/odbc.md#dicts-external_dicts_dict_sources-odbc)
-   [Табличная функция odbc](../../../engines/table-engines/integrations/odbc.md)

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/odbc/) <!--hide-->
