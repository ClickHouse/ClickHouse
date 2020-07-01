# odbc {#table-functions-odbc}

Возвращает таблицу, подключенную через [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

``` sql
odbc(connection_settings, external_database, external_table)
```

Параметры:

-   `connection_settings` — название секции с настройками соединения в файле `odbc.ini`.
-   `external_database` — имя базы данных во внешней СУБД.
-   `external_table` — имя таблицы в `external_database`.

Чтобы использование ODBC было безопасным, ClickHouse использует отдельную программу `clickhouse-odbc-bridge`. Если драйвер ODBC подгружать непосредственно из `clickhouse-server`, то проблемы с драйвером могут привести к аварийной остановке сервера ClickHouse. ClickHouse автоматически запускает `clickhouse-odbc-bridge` по мере необходимости. Программа устанавливается из того же пакета, что и `clickhouse-server`.

Поля из внешней таблицы со значениями `NULL` получают значение по умолчанию для базового типа данных. Например, если поле в удалённой таблице MySQL имеет тип `INT NULL` оно сконвертируется в 0 (значение по умолчанию для типа данных ClickHouse `Int32`).

## Пример использования {#primer-ispolzovaniia}

**Получение данных из локальной установки MySQL через ODBC**

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

Получение данных из таблицы MySQL в ClickHouse:

``` sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## Смотрите также {#smotrite-takzhe}

-   [Внешние словари ODBC](../../sql-reference/table-functions/odbc.md#dicts-external_dicts_dict_sources-odbc)
-   [Движок таблиц ODBC](../../sql-reference/table-functions/odbc.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/table_functions/jdbc/) <!--hide-->
