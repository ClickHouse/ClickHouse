---
toc_priority: 32
toc_title: SQLite
---

# SQLite {#sqlite}

Позволяет подключаться к базе данных [SQLite](https://www.sqlite.org/index.html). 

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

## Specifics and Recommendations {#specifics-and-recommendations}

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