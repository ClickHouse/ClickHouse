---
sidebar_position: 43
sidebar_label: DETACH
---

# DETACH {#detach-statement}

Заставляет сервер "забыть" о существовании таблицы, материализованного представления или словаря.

**Синтаксис**

``` sql
DETACH TABLE|VIEW|DICTIONARY [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY]
```

Такой запрос не удаляет ни данные, ни метаданные таблицы, материализованного представления или словаря. Если отключение не было перманентным (запрос без ключевого слова `PERMANENTLY`), то при следующем запуске сервер прочитает метаданные и снова узнает о таблице/представлении/словаре. Если сущность была отключена перманентно, то сервер не подключит их обратно автоматически.

Независимо от того, каким способом таблица была отключена, ее можно подключить обратно с помощью запроса [ATTACH](../../sql-reference/statements/attach.md). Системные log таблицы также могут быть подключены обратно (к примеру, `query_log`, `text_log` и др.). Другие системные таблицы не могут быть подключены обратно, но на следующем запуске сервер снова "вспомнит" об этих таблицах.

`ATTACH MATERIALIZED VIEW` не может быть использован с кратким синтаксисом (без `SELECT`), но можно подключить представление с помощью запроса `ATTACH TABLE`.

Обратите внимание, что нельзя перманентно отключить таблицу, которая уже временно отключена. Для этого ее сначала надо подключить обратно, а затем снова отключить перманентно.

Также нельзя использовать [DROP](../../sql-reference/statements/drop.md#drop-table) с отключенной таблицей или создавать таблицу с помощью [CREATE TABLE](../../sql-reference/statements/create/table.md) с таким же именем, как у отключенной таблицы. Еще нельзя заменить отключенную таблицу другой с помощью запроса [RENAME TABLE](../../sql-reference/statements/rename.md).

**Пример**

Создание таблицы:

Запрос:

``` sql
CREATE TABLE test ENGINE = Log AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

Результат:

``` text
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

Отключение таблицы:

Запрос:

``` sql
DETACH TABLE test;
SELECT * FROM test;
```

Результат:

``` text
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test doesn't exist.
```

**Смотрите также**

-   [Материализованные представления](../../sql-reference/statements/create/view.md#materialized)
-   [Словари](../../sql-reference/dictionaries/index.md)
