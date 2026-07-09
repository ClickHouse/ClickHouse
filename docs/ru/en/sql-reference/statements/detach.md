---
description: 'Документация по оператору DETACH'
sidebar_label: 'DETACH'
sidebar_position: 43
slug: /sql-reference/statements/detach
title: 'Оператор DETACH'
doc_type: 'reference'
---

Позволяет серверу &quot;забыть&quot; о существовании таблицы, materialized view, словаря или базы данных.

**Синтаксис**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

Отсоединение не удаляет данные или метаданные таблицы, materialized view, словаря или базы данных. Если сущность не была отсоединена с `PERMANENTLY`, при следующем запуске server прочитает метаданные и снова подключит таблицу/view/словарь/базу данных. Если сущность была отсоединена с `PERMANENTLY`, автоматического повторного подключения не будет.

Независимо от того, была ли таблица, словарь или база данных отсоединена навсегда или нет, в обоих случаях их можно подключить обратно с помощью запроса [ATTACH](../../sql-reference/statements/attach.md).
Системные таблицы логов также можно подключить обратно (например, `query_log`, `text_log` и т. д.). Другие системные таблицы нельзя подключить повторно. При следующем запуске server снова подключит эти таблицы.

`ATTACH MATERIALIZED VIEW` не работает с коротким синтаксисом (без `SELECT`), но её можно подключить с помощью запроса `ATTACH TABLE`.

Обратите внимание, что нельзя отсоединить навсегда таблицу, которая уже отсоединена (временно). Но её можно подключить обратно, а затем снова отсоединить навсегда.

Также нельзя выполнить [DROP](../../sql-reference/statements/drop.md#drop-table) для отсоединённой таблицы, или [CREATE TABLE](../../sql-reference/statements/create/table.md) с тем же именем, что и у таблицы, отсоединённой навсегда, или заменить её другой таблицей с помощью запроса [RENAME TABLE](../../sql-reference/statements/rename.md).

Модификатор `SYNC` выполняет действие без задержки.

**Пример**

Создание таблицы:

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
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

Отсоединение таблицы:

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

:::note
В ClickHouse Cloud следует использовать предложение `PERMANENTLY`, например: `DETACH TABLE <table> PERMANENTLY`. Если это предложение не использовать, таблицы будут снова присоединены при перезапуске кластера, например во время обновлений.
:::

**См. также**

* [Materialized View](/ru/sql-reference/statements/create/view#materialized-view)
* [Словари](./create/dictionary/overview.md)