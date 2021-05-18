---
toc_priority: 35
toc_title: PostgreSQL
---

# PostgreSQL {#postgresql}

Позволяет подключаться к БД на удаленном сервере [PostgreSQL](https://www.postgresql.org). Поддерживает операции чтения и записи (запросы `SELECT` и `INSERT`) для обмена данными между ClickHouse и PostgreSQL.

Позволяет в реальном времени получать от удаленного сервера PostgreSQL информацию о таблицах БД и их структуре с помощью запросов `SHOW TABLES` и `DESCRIBE TABLE`.

Поддерживает операции изменения структуры таблиц (`ALTER TABLE ... ADD|DROP COLUMN`). Если параметр `use_table_cache` (см. ниже раздел Параметры движка) установлен в значение `1`, структура таблицы кешируется, и изменения в структуре не отслеживаются, но будут обновлены, если выполнить команды `DETACH` и `ATTACH`.

## Создание БД {#creating-a-database}

``` sql
CREATE DATABASE test_database 
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `use_table_cache`]);
```

**Параметры движка**

-   `host:port` — адрес сервера PostgreSQL.
-   `database` — имя удаленной БД.
-   `user` — пользователь PostgreSQL.
-   `password` — пароль пользователя.
-   `use_table_cache` —  определяет кеширование структуры таблиц БД. Необязательный параметр. Значение по умолчанию: `0`.

## Поддерживаемые типы данных {#data_types-support}

| PostgerSQL       | ClickHouse                                                   |
|------------------|--------------------------------------------------------------|
| DATE             | [Date](../../sql-reference/data-types/date.md)               |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)       |
| REAL             | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)           |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md)       |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)          |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)          |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)          |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)           |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))|
| ARRAY            | [Array](../../sql-reference/data-types/array.md)             |
 

## Примеры использования {#examples-of-use}

Обмен данными между БД ClickHouse и сервером PostgreSQL:

``` sql
CREATE DATABASE test_database 
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 1);
```

``` sql
SHOW DATABASES;
```

``` text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

``` sql
SHOW TABLES FROM test_database;
```

``` text
┌─name───────┐
│ test_table │
└────────────┘
```

Чтение данных из таблицы PostgreSQL:

``` sql
SELECT * FROM test_database.test_table;
```

``` text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

Запись данных в таблицу PostgreSQL:

``` sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

Пусть структура таблицы была изменена в PostgreSQL: 

``` sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

Поскольку при создании БД параметр `use_table_cache` был установлен в значение `1`, структура таблицы в ClickHouse была кеширована и поэтому не изменилась:

``` sql
DESCRIBE TABLE test_database.test_table;
```
``` text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

После того как таблицу «отцепили» и затем снова «прицепили», структура обновилась:

``` sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```
``` text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/database-engines/postgresql/) <!--hide-->
