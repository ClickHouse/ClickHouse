---
toc_priority: 42
toc_title: postgresql
---

# postgresql {#postgresql}

Позволяет выполнять запросы `SELECT` и `INSERT` над таблицами удаленной БД PostgreSQL.

**Синтаксис**

``` sql
postgresql('host:port', 'database', 'table', 'user', 'password'[, `schema`])
```

**Аргументы**

-   `host:port` — адрес сервера PostgreSQL.
-   `database` — имя базы данных на удалённом сервере.
-   `table` — имя таблицы на удалённом сервере.
-   `user` — пользователь PostgreSQL.
-   `password` — пароль пользователя.
-   `schema` — имя схемы, если не используется схема по умолчанию. Необязательный аргумент.

**Возвращаемое значение**

Таблица с теми же столбцами, что и в исходной таблице PostgreSQL.

!!! info "Примечание"
    В запросах `INSERT` для того чтобы отличить табличную функцию `postgresql(...)` от таблицы со списком имен столбцов вы должны указывать ключевые слова `FUNCTION` или `TABLE FUNCTION`. См. примеры ниже.

## Особенности реализации {#implementation-details}

Запросы `SELECT` на стороне PostgreSQL выполняются как `COPY (SELECT ...) TO STDOUT` внутри транзакции PostgreSQL только на чтение  с коммитом после каждого запроса `SELECT`.

Простые условия для `WHERE` такие как `=`, `!=`, `>`, `>=`, `<`, `<=` и `IN` исполняются на стороне PostgreSQL сервера.

Все операции объединения, аггрегации, сортировки, условия `IN [ array ]` и ограничения `LIMIT` выполняются на стороне ClickHouse только после того как запрос к PostgreSQL закончился.

Запросы `INSERT` на стороне PostgreSQL выполняются как `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` внутри PostgreSQL транзакции с автоматическим коммитом после каждого запроса `INSERT`.

PostgreSQL массивы конвертируются в массивы ClickHouse.

!!! info "Примечание"
    Будьте внимательны, в PostgreSQL массивы, созданные как `type_name[]`, являются многомерными и могут содержать в себе разное количество измерений в разных строках одной таблицы. Внутри ClickHouse допустипы только многомерные массивы с одинаковым кол-вом измерений во всех строках таблицы.

Поддерживает несколько реплик, которые должны быть перечислены через `|`. Например:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

или

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

При использовании словаря PostgreSQL поддерживается приоритет реплик. Чем больше номер реплики, тем ниже ее приоритет. Наивысший приоритет у реплики с номером `0`.

**Примеры**

Таблица в PostgreSQL:

``` text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
 (1 row)
```

Получение данных в ClickHouse:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Вставка данных:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Using Non-default Schema:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**См. также**

-   [Движок таблиц PostgreSQL](../../sql-reference/table-functions/postgresql.md)
-   [Использование PostgreSQL как источника данных для внешнего словаря](../../sql-reference/table-functions/postgresql.md#dicts-external_dicts_dict_sources-postgresql)

[Оригинальная статья](https://clickhouse.com/docs/ru/sql-reference/table-functions/postgresql/) <!--hide-->
