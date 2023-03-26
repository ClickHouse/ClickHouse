---
sidebar_position: 32
sidebar_label: SQLite
---

# SQLite {#sqlite}

Движок баз данных позволяет подключаться к базе [SQLite](https://www.sqlite.org/index.html) и выполнять запросы `INSERT` и `SELECT` для обмена данными между ClickHouse и SQLite.

## Создание базы данных {#creating-a-database}

``` sql
    CREATE DATABASE sqlite_database 
    ENGINE = SQLite('db_path')
```

**Параметры движка**

-   `db_path` — путь к файлу с базой данных SQLite.
    
## Поддерживаемые типы данных {#data_types-support}

| SQLite       | ClickHouse                                              |
|---------------|---------------------------------------------------------|
| INTEGER       | [Int32](../../sql-reference/data-types/int-uint.md)     |
| REAL          | [Float32](../../sql-reference/data-types/float.md)      |
| TEXT          | [String](../../sql-reference/data-types/string.md)      |
| BLOB          | [String](../../sql-reference/data-types/string.md)      |

## Особенности и рекомендации {#specifics-and-recommendations}

SQLite хранит всю базу данных (определения, таблицы, индексы и сами данные) в виде единого кроссплатформенного файла на хост-машине. Во время записи SQLite блокирует весь файл базы данных, поэтому операции записи выполняются последовательно. Операции чтения могут быть многозадачными.
SQLite не требует управления службами (например, сценариями запуска) или контроля доступа на основе `GRANT` и паролей. Контроль доступа осуществляется с помощью разрешений файловой системы, предоставляемых самому файлу базы данных.

## Примеры использования {#usage-example}

Отобразим список таблиц базы данных в ClickHouse, подключенной к SQLite:

``` sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

``` text
┌──name───┐
│ table1  │
│ table2  │  
└─────────┘
```
Отобразим содержимое таблицы:

``` sql
SELECT * FROM sqlite_db.table1;
```

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```
Вставим данные в таблицу SQLite из таблицы ClickHouse:

``` sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```
