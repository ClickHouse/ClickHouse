---
toc_priority: 43
toc_title: DETACH
---

# DETACH {#detach-statement}

Удаляет из сервера информацию о таблице или материализованном представлении. Сервер перестаёт знать о существовании таблицы.

Синтаксис:

``` sql
DETACH TABLE|VIEW [IF EXISTS] [db.]name [PERMANENTLY] [ON CLUSTER cluster]
```

Но ни данные, ни метаданные таблицы или материализованного представления не удаляются. При следующем запуске сервера, если не было использовано `PERMANENTLY`, сервер прочитает метаданные и снова узнает о таблице/представлении. Если таблица или представление были откреплено перманентно, сервер не прикрепит их обратно автоматически.

Независимо от того, каким способом таблица была откреплена, ее можно прикрепить обратно с помощью запроса [ATTACH](../../sql-reference/statements/attach.md). Системные log таблицы также могут быть прикреплены обратно (к примеру `query_log`, `text_log` и др.) Другие системные таблицы не могут быть прикреплены обратно, но на следующем запуске сервер снова вспомнит об этих таблицах.

`ATTACH MATERIALIZED VIEW` не может быть использован с кратким синтаксисом (без `SELECT`), но можно прикрепить представление с помощью запроса `ATTACH TABLE`.

Обратите внимание, что нельзя перманентно открепить таблицу, которая уже временно откреплена. Для этого ее сначала надо прикрепить обратно, а затем снова открепить перманентно.

Также нельзя использовать [DROP](../../sql-reference/statements/drop.md#drop-table) с открепленной таблицей или создавать таблицу с помощью [CREATE TABLE](../../sql-reference/statements/create/table.md) с таким же именем, как уже открепленная таблица. Еще нельзя заменить открепленную таблицу другой с помощью запроса [RENAME TABLE](../../sql-reference/statements/rename.md).

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

Открепление таблицы:

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

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/detach/) <!--hide-->
