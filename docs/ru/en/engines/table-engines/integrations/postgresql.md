---
description: 'Движок PostgreSQL позволяет выполнять запросы `SELECT` и `INSERT` к данным, хранящимся
  на удалённом сервере PostgreSQL.'
sidebar_label: 'PostgreSQL'
sidebar_position: 160
slug: /engines/table-engines/integrations/postgresql
title: 'Движок таблицы PostgreSQL'
doc_type: 'guide'
---

Движок PostgreSQL позволяет выполнять запросы `SELECT` и `INSERT` к данным, хранящимся на удалённом сервере PostgreSQL.

:::note
В настоящее время для движка таблицы поддерживаются только PostgreSQL версии 12 и выше.
:::

:::tip
Ознакомьтесь с нашим сервисом [Managed Postgres](/ru/docs/cloud/managed-postgres). Благодаря NVMe-хранилищу, физически расположенному рядом с вычислительными ресурсами, он обеспечивает до 10 раз более высокую производительность для рабочих нагрузок, ограниченных производительностью дисковой подсистемы, по сравнению с альтернативами, использующими сетевое хранилище, такими как EBS, а также позволяет реплицировать данные из Postgres в ClickHouse с помощью коннектора Postgres CDC в ClickPipes.
:::

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

См. подробное описание запроса [CREATE TABLE](/ru/sql-reference/statements/create/table).

Структура таблицы может отличаться от структуры исходной таблицы PostgreSQL:

* Имена столбцов должны совпадать с именами в исходной таблице PostgreSQL, но можно использовать только часть этих столбцов и в любом порядке.
* Типы столбцов могут отличаться от типов в исходной таблице PostgreSQL. ClickHouse пытается [привести](../../../engines/database-engines/postgresql.md#data_types-support) значения к типам данных ClickHouse.
* Настройка [external&#95;table&#95;functions&#95;use&#95;nulls](/ru/operations/settings/settings#external_table_functions_use_nulls) определяет, как обрабатывать столбцы с типом Nullable. Значение по умолчанию: 1. Если задано 0, табличная функция не создает столбцы с типом Nullable и вставляет значения по умолчанию вместо NULL. Это также применимо к значениям NULL внутри массивов.

**Параметры движка**

* `host:port` — адрес PostgreSQL-сервера.
* `database` — имя удаленной базы данных.
* `table` — имя удаленной таблицы или запрос, передаваемый в PostgreSQL как есть (см. [Передача запроса вместо имени таблицы](#passing-a-query)).
* `user` — пользователь PostgreSQL.
* `password` — пароль пользователя.
* `schema` — схема таблицы, отличная от используемой по умолчанию. Необязательно.
* `on_conflict` — стратегия разрешения конфликтов. Пример: `ON CONFLICT DO NOTHING`. Необязательно. Примечание: добавление этого параметра снизит эффективность вставки.

Для среды продакшн рекомендуется использовать [именованные коллекции](/ru/operations/named-collections.md) (доступно начиная с версии 21.11). Вот пример:

```xml
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

Некоторые параметры можно переопределить с помощью аргументов в формате ключ-значение:

```sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

<div id="implementation-details">
  ## Подробности реализации
</div>

`SELECT`-запросы со стороны PostgreSQL выполняются как `COPY (SELECT ...) TO STDOUT` внутри транзакции PostgreSQL в режиме только для чтения, с коммитом после каждого `SELECT`-запроса.

Простые предложения `WHERE`, такие как `=`, `!=`, `>`, `>=`, `<`, `<=` и `IN`, выполняются на сервере PostgreSQL.

Все JOIN, агрегации, сортировка, условия `IN [ array ]` и ограничение сэмплирования `LIMIT` выполняются в ClickHouse только после того, как запрос к PostgreSQL завершится.

<div id="passing-a-query">
  ## Передача запроса вместо имени таблицы
</div>

Вместо имени таблицы аргумент `table` может содержать запрос `SELECT`, который передаётся в PostgreSQL как есть. Структура таблицы определяется по результату запроса. Запрос можно записать либо как подзапрос, либо обернуть в функцию `query`:

```sql
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Это позволяет проталкивать JOIN, агрегации и любую другую обработку в PostgreSQL. Такая таблица доступна только для чтения: `INSERT` в неё не поддерживается. Тот же синтаксис поддерживается табличной функцией [`postgresql`](/ru/sql-reference/table-functions/postgresql).

:::note
Форма с подзапросом `(SELECT ...)` разбирается ClickHouse и повторно сериализуется в диалекте PostgreSQL (с экранированием идентификаторов PostgreSQL и строковых литералов) перед отправкой на сервер. Поэтому она должна быть корректным ClickHouse SQL. Чтобы передать синтаксис, специфичный для PostgreSQL, который ClickHouse не разбирает, используйте форму `query('...')`: её текст отправляется в PostgreSQL дословно.

Любой внешний `WHERE`, `LIMIT`, агрегация и т. д. из окружающего запроса к ClickHouse **не** проталкиваются в переданный запрос — они применяются в ClickHouse после получения полного результата запроса. Чтобы ограничить объём данных, считываемых из PostgreSQL, поместите фильтр внутрь передаваемого запроса. При [`external_table_strict_query = 1`](/ru/operations/settings/settings#external_table_strict_query) внешний фильтр, который нельзя протолкнуть, вместо локального применения отклоняется с исключением.
:::

Запросы `INSERT` на стороне PostgreSQL выполняются как `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` внутри транзакции PostgreSQL с автоматическим коммитом после каждого оператора `INSERT`.

Типы PostgreSQL `Array` преобразуются в массивы ClickHouse.

:::note
Будьте внимательны: в PostgreSQL массив, созданный как `type_name[]`, может содержать многомерные массивы с разным числом измерений в разных строках одного и того же столбца. В ClickHouse же допускаются только многомерные массивы с одинаковым числом измерений во всех строках одного и того же столбца.
:::

Поддерживается несколько реплик, которые должны быть перечислены через `|`. Например:

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

Поддерживается задание приоритетов реплик для источника словаря PostgreSQL. Чем больше число в map, тем ниже приоритет. Наивысший приоритет — `0`.

В примере ниже у реплики `example01-1` наивысший приоритет:

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

<div id="usage-example">
  ## Пример использования
</div>

<div id="table-in-postgresql">
  ### Таблица в PostgreSQL
</div>

```text
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

<div id="creating-table-in-clickhouse-and-connecting-to--postgresql-table-created-above">
  ### Создание таблицы в ClickHouse и подключение к таблице PostgreSQL, созданной выше
</div>

В этом примере используется [движок таблицы PostgreSQL](/ru/engines/table-engines/integrations/postgresql.md), чтобы связать таблицу ClickHouse с таблицей PostgreSQL и выполнять в базе данных PostgreSQL команды SELECT и INSERT:

```sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-initial-data-from-postgresql-table-into-clickhouse-table-using-a-select-query">
  ### Вставка начальных данных из таблицы PostgreSQL в таблицу ClickHouse с помощью запроса SELECT
</div>

[Табличная функция postgresql](/ru/sql-reference/table-functions/postgresql.md) копирует данные из PostgreSQL в ClickHouse. Ее часто используют, чтобы повысить производительность запросов, выполняя запросы и аналитику в ClickHouse вместо PostgreSQL; также она подходит для миграции данных из PostgreSQL в ClickHouse. Поскольку мы будем копировать данные из PostgreSQL в ClickHouse, в качестве движка таблицы в ClickHouse мы будем использовать MergeTree и назовем таблицу postgresql&#95;copy:

```sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-incremental-data-from-postgresql-table-into-clickhouse-table">
  ### Вставка инкрементальных данных из таблицы PostgreSQL в таблицу ClickHouse
</div>

Если после первоначальной вставки требуется выполнять дальнейшую синхронизацию между таблицей PostgreSQL и таблицей ClickHouse, в ClickHouse можно использовать предложение WHERE, чтобы вставлять только те данные, которые были добавлены в PostgreSQL, на основе временной метки или уникального идентификатора последовательности.

Для этого потребуется отслеживать максимальный ID или временную метку, добавленные ранее, например:

```sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

Затем выполняется вставка значений из таблицы PostgreSQL, превышающих максимальное значение

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password')
WHERE int_id > (SELECT max(int_id) FROM default.postgresql_copy);
```

<div id="selecting-data-from-the-resulting-clickhouse-table">
  ### Выборка данных из итоговой таблицы ClickHouse
</div>

```sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

```text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

<div id="using-non-default-schema">
  ### Использование нестандартной схемы
</div>

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

* [Табличная функция `postgresql`](../../../sql-reference/table-functions/postgresql.md)
* [Использование PostgreSQL в качестве источника словаря](/ru/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="related-content">
  ## Материалы по теме
</div>

* Блог: [ClickHouse и PostgreSQL — идеальное сочетание в мире данных — часть 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* Блог: [ClickHouse и PostgreSQL — идеальное сочетание в мире данных — часть 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)