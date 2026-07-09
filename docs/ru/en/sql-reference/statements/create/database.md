---
description: 'Документация по CREATE DATABASE'
sidebar_label: 'DATABASE'
sidebar_position: 35
slug: /sql-reference/statements/create/database
title: 'CREATE DATABASE'
doc_type: 'reference'
---

Создает новую базу данных.

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
```

<div id="clauses">
  ## Секции
</div>

<div id="if-not-exists">
  ### IF NOT EXISTS
</div>

Если база данных `db_name` уже существует, ClickHouse не создаёт новую базу данных:

* не генерирует исключение, если указано это условие;
* генерирует исключение, если это условие не указано.

<div id="on-cluster">
  ### ON CLUSTER
</div>

ClickHouse создаёт базу данных `db_name` на всех серверах указанного кластера. Подробнее см. в статье [Distributed DDL](../../../sql-reference/distributed-ddl.md).

<div id="engine">
  ### ДВИЖОК
</div>

По умолчанию ClickHouse использует собственный [Atomic](../../../engines/database-engines/atomic.md) движок базы данных. Также есть [MySQL](../../../engines/database-engines/mysql.md), [PostgresSQL](../../../engines/database-engines/postgresql.md), [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md), [Replicated](../../../engines/database-engines/replicated.md), [SQLite](../../../engines/database-engines/sqlite.md).

<div id="comment">
  ### COMMENT
</div>

При создании базы данных можно добавить к ней комментарий.

Комментарий поддерживается всеми движками баз данных.

**Синтаксис**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**Пример**

```sql title="Query"
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

```text title="Response"
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

<div id="settings">
  ### НАСТРОЙКИ
</div>

<div id="lazy-load-tables">
  #### lazy_load_tables
</div>

Если включено, таблицы не загружаются полностью при запуске базы данных. Вместо этого для каждой таблицы создается легковесный прокси, а реальный движок таблицы материализуется при первом обращении к ней. Это сокращает время запуска и использование памяти для баз данных с большим количеством таблиц, из которых активно запрашивается лишь часть.

```sql
CREATE DATABASE db_name ENGINE = Atomic SETTINGS lazy_load_tables = 1;
```

Применяется к движкам баз данных, которые хранят метаданные таблиц на диске (например, `Atomic`, `Ordinary`). Представления, materialized view, словари и таблицы на основе табличных функций всегда загружаются немедленно независимо от этого параметра.

**Когда использовать:** Этот параметр полезен для баз данных с большим количеством таблиц (сотни или тысячи), к которым активно обращаются лишь к части. Он сокращает время запуска сервера и использование памяти, откладывая создание объектов движка таблицы, сканирование частей данных и инициализацию фоновых потоков до первого обращения.

**Влияние на `system.tables`:**

* До обращения к таблице `system.tables` показывает её движок как `TableProxy`. После первого обращения отображается реальное имя движка (например, `MergeTree`).
* Столбцы, такие как `total_rows` и `total_bytes`, возвращают `NULL` для незагруженных таблиц, потому что реальное хранилище ещё не создано.

**Взаимодействие с DDL-операциями:**

* `SELECT`, `INSERT`, `ALTER`, `DROP` при первом использовании автоматически запускают загрузку реального движка таблицы.
* `RENAME TABLE` работает без запуска загрузки.
* После загрузки таблица остаётся загруженной на всё время работы серверного процесса.

**Ограничения:**

* Средства мониторинга, которые полагаются на метаданные `system.tables` (например, `total_rows`, `engine`), могут видеть неполную информацию для незагруженных таблиц.
* Первый запрос к незагруженной таблице приводит к разовым затратам на загрузку (разбор сохранённого оператора `CREATE TABLE` и инициализация движка).

Значение по умолчанию: `0` (отключено).