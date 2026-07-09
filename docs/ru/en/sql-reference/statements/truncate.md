---
description: 'Документация по командам TRUNCATE'
sidebar_label: 'TRUNCATE'
sidebar_position: 52
slug: /sql-reference/statements/truncate
title: 'Команды TRUNCATE'
doc_type: 'reference'
---

Оператор `TRUNCATE` в ClickHouse используется для быстрого удаления всех данных из таблицы или базы данных с сохранением их структуры.

<div id="truncate-table">
  ## TRUNCATE TABLE
</div>

```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<br />

| Параметр             | Описание                                                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `IF EXISTS`          | Предотвращает ошибку, если таблица не существует. Если это условие опущено, запрос вернет ошибку.                                                     |
| `db.name`            | Необязательное имя базы данных.                                                                                                                       |
| `ON CLUSTER cluster` | Выполняет команду на указанном кластере.                                                                                                              |
| `SYNC`               | Делает очистку синхронной на репликах при использовании реплицируемых таблиц. Если этот параметр опущен, по умолчанию очистка выполняется асинхронно. |

Вы можете использовать настройку [alter&#95;sync](/ru/operations/settings/settings#alter_sync), чтобы настроить ожидание выполнения действий на репликах.

Вы можете указать, как долго (в секундах) ждать выполнения запросов `TRUNCATE` неактивными репликами, с помощью настройки [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ru/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Если для `alter_sync` задано значение `2` и некоторые реплики остаются неактивными дольше времени, указанного в настройке `replication_wait_for_inactive_replica_timeout`, будет сгенерировано исключение `UNFINISHED`.
:::

Запрос `TRUNCATE TABLE` **не поддерживается** для следующих движков таблиц:

* [`View`](../../engines/table-engines/special/view.md)
* [`File`](../../engines/table-engines/special/file.md)
* [`URL`](../../engines/table-engines/special/url.md)
* [`Buffer`](../../engines/table-engines/special/buffer.md)
* [`Null`](../../engines/table-engines/special/null.md)

<div id="truncate-all-tables">
  ## TRUNCATE ВСЕХ ТАБЛИЦ
</div>

```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```

<br />

| Parameter                               | Описание                                              |
| --------------------------------------- | ----------------------------------------------------- |
| `ALL`                                   | Удаляет данные из всех таблиц базы данных.            |
| `IF EXISTS`                             | Предотвращает ошибку, если база данных не существует. |
| `db`                                    | Имя базы данных.                                      |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | Фильтрует таблицы по шаблону.                         |
| `ON CLUSTER cluster`                    | Выполняет команду на всех узлах кластера.             |

Удаляет все данные из всех таблиц базы данных.

<div id="truncate-database">
  ## TRUNCATE DATABASE
</div>

```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

<br />

| Parameter            | Description                                           |
| -------------------- | ----------------------------------------------------- |
| `IF EXISTS`          | Предотвращает ошибку, если база данных не существует. |
| `db`                 | Имя базы данных.                                      |
| `ON CLUSTER cluster` | Выполняет команду в указанном кластере.               |

Удаляет все table из database, но сохраняет саму database. Если предложение `IF EXISTS` опущено, запрос вернет ошибку, если database не существует.

:::note
`TRUNCATE DATABASE` не поддерживается для баз данных `Replicated`. Вместо этого просто удалите database с помощью `DROP` и заново создайте ее с помощью `CREATE`.
:::