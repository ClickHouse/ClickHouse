---
description: 'Позволяет подключаться к базам данных SQLite и выполнять запросы `INSERT` и `SELECT`
  для обмена данными между ClickHouse и SQLite.'
sidebar_label: 'SQLite'
sidebar_position: 55
slug: /engines/database-engines/sqlite
title: 'SQLite'
doc_type: 'reference'
---

Позволяет подключаться к базе данных [SQLite](https://www.sqlite.org/index.html) и выполнять запросы `INSERT` и `SELECT` для обмена данными между ClickHouse и SQLite.

<div id="creating-a-database">
  ## Создание базы данных
</div>

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**Параметры движка**

* `db_path` — Путь к файлу базы данных SQLite.

<div id="data_types-support">
  ## Поддержка типов данных
</div>

В таблице ниже показано сопоставление типов по умолчанию, когда ClickHouse автоматически выводит схему из SQLite:

| SQLite  | ClickHouse                                          |
| ------- | --------------------------------------------------- |
| INTEGER | [Int32](../../sql-reference/data-types/int-uint.md) |
| REAL    | [Float32](../../sql-reference/data-types/float.md)  |
| TEXT    | [String](../../sql-reference/data-types/string.md)  |
| TEXT    | [UUID](../../sql-reference/data-types/uuid.md)      |
| BLOB    | [String](../../sql-reference/data-types/string.md)  |

Если вы явно определяете таблицу с конкретными типами ClickHouse, используя [движок таблицы SQLite](../../engines/table-engines/integrations/sqlite.md), из столбцов SQLite типа TEXT можно разобрать следующие типы ClickHouse:

* [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
* [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
* [UUID](../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
* [FixedString](../../sql-reference/data-types/fixedstring.md)
* Все целочисленные типы ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../sql-reference/data-types/float.md)

SQLite использует динамическую типизацию, а его функции чтения значений выполняют автоматическое приведение типов. Например, при чтении столбца TEXT как целого числа будет возвращено 0, если текст нельзя разобрать как число. Это означает, что если для таблицы ClickHouse задан тип, отличный от типа базового столбца SQLite, значения могут неявно приводиться вместо того, чтобы вызывать ошибку.

<div id="specifics-and-recommendations">
  ## Особенности и рекомендации
</div>

SQLite хранит всю базу данных (определения, таблицы, индексы и сами данные) в виде одного кроссплатформенного файла на хост-машине. Во время записи SQLite блокирует весь файл базы данных, поэтому операции записи выполняются последовательно. Операции чтения могут выполняться параллельно.
SQLite не требует управления сервисом (например, скриптов запуска) или управления доступом на основе `GRANT` и паролей. Управление доступом осуществляется с помощью разрешений файловой системы, назначенных самому файлу базы данных.

<div id="usage-example">
  ## Пример использования
</div>

База данных в ClickHouse, подключённая к SQLite:

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

Отображает таблицы:

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

Вставка данных из таблицы ClickHouse в таблицу SQLite:

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```