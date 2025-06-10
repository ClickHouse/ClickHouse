---
slug: /ru/engines/table-engines/integrations/postgresql
sidebar_position: 11
sidebar_label: PostgreSQL
---

#PostgreSQL {#postgresql}

Движок PostgreSQL позволяет выполнять запросы `SELECT` и `INSERT` для таблиц на удаленном сервере PostgreSQL.

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = PostgreSQL('host:port', 'database', 'table', 'user', 'password'[, `schema`]);
```

Смотрите подробное описание запроса [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query).

Структура таблицы может отличаться от структуры исходной таблицы PostgreSQL:

-   Имена столбцов должны быть такими же, как в исходной таблице PostgreSQL, но можно использовать только некоторые из этих столбцов и в любом порядке.
-   Типы столбцов могут отличаться от типов в исходной таблице PostgreSQL. ClickHouse пытается [привести](../../../engines/database-engines/postgresql.md#data_types-support) значения к типам данных ClickHouse.
-   Настройка [external_table_functions_use_nulls](../../../operations/settings/settings.md#external-table-functions-use-nulls) определяет как обрабатывать Nullable столбцы. Значение по умолчанию: 1. Если значение 0, то табличная функция не делает Nullable столбцы, а вместо NULL выставляет значения по умолчанию для скалярного типа. Это также применимо для значений NULL внутри массивов.

**Параметры движка**

-   `host:port` — адрес сервера PostgreSQL.
-   `database` — имя базы данных на сервере PostgreSQL.
-   `table` — имя таблицы.
-   `user` — имя пользователя PostgreSQL.
-   `password` — пароль пользователя PostgreSQL.
-   `schema` — имя схемы, если не используется схема по умолчанию. Необязательный аргумент.

## Особенности реализации {#implementation-details}

Запросы `SELECT` на стороне PostgreSQL выполняются как `COPY (SELECT ...) TO STDOUT` внутри транзакции PostgreSQL только на чтение с коммитом после каждого запроса `SELECT`.

Простые условия для `WHERE`, такие как `=`, `!=`, `>`, `>=`, `<`, `<=` и `IN`, исполняются на стороне PostgreSQL сервера.

Все операции объединения, аггрегации, сортировки, условия `IN [ array ]` и ограничения `LIMIT` выполняются на стороне ClickHouse только после того, как запрос к PostgreSQL закончился.

Запросы `INSERT` на стороне PostgreSQL выполняются как `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` внутри PostgreSQL транзакции с автоматическим коммитом после каждого запроса `INSERT`.

PostgreSQL массивы конвертируются в массивы ClickHouse.

:::info Внимание
Будьте внимательны, в PostgreSQL массивы, созданные как `type_name[]`, являются многомерными и могут содержать в себе разное количество измерений в разных строках одной таблицы. Внутри ClickHouse допустимы только многомерные массивы с одинаковым кол-вом измерений во всех строках таблицы.
:::

Поддерживает несколько реплик, которые должны быть перечислены через `|`. Например:

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

При использовании словаря PostgreSQL поддерживается приоритет реплик. Чем больше номер реплики, тем ниже ее приоритет. Наивысший приоритет у реплики с номером `0`.

В примере ниже реплика `example01-1` имеет более высокий приоритет:

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

## Пример использования {#usage-example}

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

Таблица в ClickHouse, получение данных из PostgreSQL таблицы, созданной выше:

``` sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
```

``` sql
SELECT * FROM postgresql_table WHERE str IN ('test');
```

``` text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
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

-   [Табличная функция `postgresql`](../../../sql-reference/table-functions/postgresql.md)
-   [Использование PostgreSQL в качестве источника для внешнего словаря](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)
