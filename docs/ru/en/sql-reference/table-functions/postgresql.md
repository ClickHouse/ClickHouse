---
description: 'Позволяет выполнять запросы `SELECT` и `INSERT` к данным, хранящимся
  на удалённом сервере PostgreSQL.'
sidebar_label: 'postgresql'
sidebar_position: 160
slug: /sql-reference/table-functions/postgresql
title: 'postgresql'
doc_type: 'справочник'
---

Позволяет выполнять запросы `SELECT` и `INSERT` к данным, хранящимся на удалённом сервере PostgreSQL.

<div id="syntax">
  ## Синтаксис
</div>

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## Аргументы
</div>

| Аргумент      | Описание                                                                                                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | Адрес сервера PostgreSQL.                                                                                                             |
| `database`    | Имя удалённой базы данных.                                                                                                            |
| `table`       | Имя удалённой таблицы или запрос, передаваемый в PostgreSQL как есть (см. [Передача запроса вместо имени таблицы](#passing-a-query)). |
| `user`        | Пользователь PostgreSQL.                                                                                                              |
| `password`    | Пароль пользователя.                                                                                                                  |
| `schema`      | Схема таблицы, отличная от используемой по умолчанию. Необязательно.                                                                  |
| `on_conflict` | Стратегия разрешения конфликтов. Пример: `ON CONFLICT DO NOTHING`. Необязательно.                                                     |

Аргументы также можно передавать с помощью [именованных коллекций](/ru/operations/named-collections.md). В этом случае `host` и `port` нужно указывать отдельно. Этот подход рекомендуется для продакшна.

<div id="returned_value">
  ## Возвращаемое значение
</div>

Объект table с теми же столбцами, что и у исходной таблицы PostgreSQL.

:::note
В запросе `INSERT`, чтобы отличить табличную функцию `postgresql(...)` от имени таблицы со списком имён столбцов, необходимо использовать ключевые слова `FUNCTION` или `TABLE FUNCTION`. См. примеры ниже.
:::

<div id="implementation-details">
  ## Подробности реализации
</div>

Запросы `SELECT` на стороне PostgreSQL выполняются как `COPY (SELECT ...) TO STDOUT` внутри транзакции PostgreSQL в режиме только для чтения, с коммитом после каждого запроса `SELECT`.

Простые условия предложения `WHERE`, такие как `=`, `!=`, `>`, `>=`, `<`, `<=` и `IN`, выполняются на сервере PostgreSQL.

Все операции JOIN, агрегации, сортировка, условия `IN [ array ]` и ограничение сэмплирования `LIMIT` выполняются в ClickHouse только после завершения запроса к PostgreSQL.

<div id="passing-a-query">
  ## Передача запроса вместо имени таблицы
</div>

Вместо имени таблицы в качестве третьего аргумента можно указать запрос `SELECT`, который передаётся в PostgreSQL в неизменном виде. Структура результирующей таблицы определяется по результату запроса. Запрос можно записать либо как подзапрос, либо обернуть в функцию `query`:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Это полезно, чтобы проталкивать JOIN, агрегации и любую другую обработку в PostgreSQL. Такая таблица доступна только для чтения: `INSERT` в неё не разрешён. Тот же синтаксис поддерживается движком таблицы [`PostgreSQL`](/ru/engines/table-engines/integrations/postgresql).

:::note
Форма с подзапросом `(SELECT ...)` разбирается ClickHouse и повторно сериализуется в диалект PostgreSQL (с экранированием идентификаторов PostgreSQL и строковых литералов) перед отправкой на сервер. Следовательно, она должна быть корректным ClickHouse SQL. Чтобы передать синтаксис, специфичный для PostgreSQL, который ClickHouse не разбирает, используйте форму `query('...')`, текст которой отправляется в PostgreSQL дословно.

Любое внешнее `предложение WHERE`, `LIMIT`, агрегация и т. д. из окружающего запроса к ClickHouse **не** проталкивается в переданный запрос — оно применяется в ClickHouse после получения полного результата запроса. Чтобы ограничить объём данных, читаемых из PostgreSQL, поместите фильтр внутрь переданного запроса. При [`external_table_strict_query = 1`](/ru/operations/settings/settings#external_table_strict_query) внешний фильтр, который нельзя протолкнуть, отклоняется с исключением вместо локального применения.
:::

Запросы `INSERT` на стороне PostgreSQL выполняются как `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` внутри транзакции PostgreSQL с автокоммитом после каждого оператора `INSERT`.

Типы PostgreSQL Array преобразуются в массивы ClickHouse.

:::note
Будьте внимательны: в PostgreSQL столбец с типом массива, такой как Integer[], может содержать массивы разной размерности в разных строках, но в ClickHouse допускаются только многомерные массивы одинаковой размерности во всех строках.
:::

Поддерживается несколько реплик, которые должны быть перечислены через `|`. Например:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

or

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

Поддерживается приоритет реплик для источника словаря PostgreSQL. Чем больше число в map, тем ниже приоритет. Наивысший приоритет — `0`.

<div id="examples">
  ## Примеры
</div>

Таблица в PostgreSQL:

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

Выборка данных из ClickHouse с помощью простых аргументов:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

Или с помощью [именованных коллекций](/ru/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Вставка:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Использование схемы, отличной от default:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

<div id="related">
  ## См. также
</div>

* [Движок таблицы PostgreSQL](../../engines/table-engines/integrations/postgresql.md)
* [Использование PostgreSQL в качестве источника словаря](/ru/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="replicating-or-migrating-postgres-data-with-peerdb">
  ### Репликация или миграция данных из Postgres с помощью PeerDB
</div>

> Помимо табличных функций, вы всегда можете использовать [PeerDB](https://docs.peerdb.io/introduction) от ClickHouse, чтобы настроить непрерывный конвейер передачи данных из Postgres в ClickHouse. PeerDB — это инструмент, специально разработанный для репликации данных из Postgres в ClickHouse с использованием CDC (фиксации изменений данных).