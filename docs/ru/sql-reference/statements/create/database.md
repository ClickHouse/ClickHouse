---
toc_priority: 35
toc_title: "База данных"
---

# CREATE DATABASE {#query-language-create-database}

Создает базу данных.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [COMMENT 'Comment']
```

## Секции {#clauses}

### IF NOT EXISTS {#if-not-exists}

Если база данных с именем `db_name` уже существует, то ClickHouse не создает базу данных и:

-   Не генерирует исключение, если секция указана.
-   Генерирует исключение, если секция не указана.

### ON CLUSTER {#on-cluster}

ClickHouse создаёт базу данных с именем `db_name` на всех серверах указанного кластера. Более подробную информацию смотрите в разделе [Распределенные DDL запросы](../../../sql-reference/distributed-ddl.md).

### ENGINE {#engine}

По умолчанию ClickHouse использует собственный движок баз данных [Atomic](../../../engines/database-engines/atomic.md). Есть также движки баз данных [Lazy](../../../engines/database-engines/lazy.md), [MySQL](../../../engines/database-engines/mysql.md), [PostgresSQL](../../../engines/database-engines/postgresql.md), [MaterializedMySQL](../../../engines/database-engines/materialized-mysql.md), [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md), [Replicated](../../../engines/database-engines/replicated.md), [SQLite](../../../engines/database-engines/sqlite.md).

### COMMENT {#comment}

Вы можете добавить комментарий к базе данных при ее создании.

Комментарий поддерживается для всех движков баз данных.

**Синтаксис**

``` sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**Пример**

Запрос:

``` sql
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

Результат:

```text
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```
