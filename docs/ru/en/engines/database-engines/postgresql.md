---
description: 'Позволяет подключаться к базам данных на удалённом сервере PostgreSQL.'
sidebar_label: 'PostgreSQL'
sidebar_position: 40
slug: /engines/database-engines/postgresql
title: 'PostgreSQL'
doc_type: 'guide'
---

Позволяет подключаться к базам данных на удалённом [PostgreSQL](https://www.postgresql.org) сервере. Поддерживает операции чтения и записи (запросы `SELECT` и `INSERT`) для обмена данными между ClickHouse и PostgreSQL.

Обеспечивает доступ в реальном времени к списку таблиц и их структуре на удалённом сервере PostgreSQL с помощью запросов `SHOW TABLES` и `DESCRIBE TABLE`.

Поддерживает изменение структуры таблиц (`ALTER TABLE ... ADD|DROP COLUMN`). Если параметр `use_table_cache` (см. раздел «Параметры движка» ниже) установлен в `1`, структура таблицы кэшируется, и изменения в ней не проверяются, но её можно обновить с помощью запросов `DETACH` и `ATTACH`.

<div id="creating-a-database">
  ## Создание базы данных
</div>

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**Параметры движка**

* `host:port` — Адрес сервера PostgreSQL.
* `database` — Имя удалённой базы данных.
* `user` — Пользователь PostgreSQL.
* `password` — Пароль пользователя.
* `schema` — Схема PostgreSQL.
* `use_table_cache` — Определяет, кэшируется ли структура таблицы базы данных. Необязательный параметр. Значение по умолчанию: `0`.

<div id="data_types-support">
  ## Поддержка типов данных
</div>

| PostgreSQL       | ClickHouse                                                                 |
| ---------------- | -------------------------------------------------------------------------- |
| DATE             | [Date](../../sql-reference/data-types/date.md)                             |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)                     |
| REAL             | [Float32](../../sql-reference/data-types/float.md)                         |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)                         |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md) (см. примечание ниже) |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)                        |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)                        |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)                        |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)                       |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)                       |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)                         |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))              |
| ARRAY            | [Array](../../sql-reference/data-types/array.md)                           |

:::note
Для PostgreSQL `numeric(p, 0)` с точностью `p` больше 76 (максимум, поддерживаемый `Decimal256`) — например, `numeric(78, 0)`, который обычно используется для хранения 256-битных целых чисел, — вместо `Decimal` используется [`Int256`](../../sql-reference/data-types/int-uint.md). Значения, не помещающиеся в диапазон `Int256`, отклоняются с ошибкой.
:::

<div id="examples-of-use">
  ## Примеры использования
</div>

База данных в ClickHouse, обменивающаяся данными с сервером PostgreSQL:

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

```sql
SHOW DATABASES;
```

```text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

```sql
SHOW TABLES FROM test_database;
```

```text
┌─name───────┐
│ test_table │
└────────────┘
```

Чтение данных из таблицы PostgreSQL:

```sql
SELECT * FROM test_database.test_table;
```

```text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

Запись данных в таблицу PostgreSQL:

```sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

Предположим, что структура таблицы в PostgreSQL была изменена:

```sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

Поскольку при создании базы данных параметр `use_table_cache` был установлен в `1`, структура таблицы в ClickHouse была сохранена в кэше и поэтому не изменилась:

```sql
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

После отключения таблицы и её повторного подключения структура была обновлена:

```sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [ClickHouse и PostgreSQL — идеальная пара в мире данных — часть 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* Блог: [ClickHouse и PostgreSQL — идеальная пара в мире данных — часть 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)