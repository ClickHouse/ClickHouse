---
description: 'Движок `Atomic` поддерживает неблокирующие запросы `DROP TABLE` и `RENAME TABLE`,
  а также атомарные запросы `EXCHANGE TABLES`. Движок базы данных `Atomic`
  используется по умолчанию.'
sidebar_label: 'Atomic'
sidebar_position: 10
slug: /engines/database-engines/atomic
title: 'Atomic'
doc_type: 'reference'
---

Движок `Atomic` поддерживает неблокирующие запросы [`DROP TABLE`](#drop-detach-table) и [`RENAME TABLE`](#rename-table), а также атомарные запросы [`EXCHANGE TABLES`](#exchange-tables). Движок базы данных `Atomic` по умолчанию используется в ClickHouse с открытым исходным кодом.

:::note
В ClickHouse Cloud по умолчанию используется [движок базы данных `Shared`](/ru/cloud/reference/shared-catalog#shared-database-engine), который также поддерживает
перечисленные выше операции.
:::

<div id="creating-a-database">
  ## Создание базы данных
</div>

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

<div id="specifics-and-recommendations">
  ## Особенности и рекомендации
</div>

<div id="table-uuid">
  ### UUID таблицы
</div>

Каждая таблица в базе данных `Atomic` имеет постоянный [UUID](../../sql-reference/data-types/uuid.md) и хранит данные в следующем каталоге:

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

Где `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` — UUID таблицы.

По умолчанию UUID создаётся автоматически. Однако пользователи могут явно указать UUID при создании таблицы, хотя делать это не рекомендуется.

Например:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
Вы можете использовать настройку [show&#95;table&#95;uuid&#95;in&#95;table&#95;create&#95;query&#95;if&#95;not&#95;nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil), чтобы выводить UUID в результате запроса `SHOW CREATE`.
:::

<div id="rename-table">
  ### RENAME TABLE
</div>

Команды [`RENAME`](../../sql-reference/statements/rename.md) не изменяют UUID и не перемещают данные таблицы. Эти команды выполняются сразу и не ожидают завершения других запросов, использующих таблицу.

<div id="drop-detach-table">
  ### DROP/DETACH TABLE
</div>

При использовании `DROP TABLE` данные не удаляются. Движок `Atomic` лишь помечает таблицу как удалённую, перемещая её метаданные в `/clickhouse_path/metadata_dropped/`, и уведомляет фоновый поток. Задержка перед окончательным удалением данных таблицы задаётся настройкой [`database_atomic_delay_before_drop_table_sec`](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec).
Вы можете включить синхронный режим с помощью модификатора `SYNC`. Для этого используйте настройку [`database_atomic_wait_for_drop_and_detach_synchronously`](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously). В этом случае `DROP` ждёт завершения выполняющихся `SELECT`, `INSERT` и других запросов, использующих таблицу. Таблица будет удалена, когда перестанет использоваться.

<div id="exchange-tables">
  ### EXCHANGE TABLES/СЛОВАРИ
</div>

Запрос [`EXCHANGE`](../../sql-reference/statements/exchange.md) атомарно меняет местами таблицы или словари. Например, вместо этой неатомарной операции:

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```

можно использовать базу данных atomic:

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

<div id="replicatedmergetree-in-atomic-database">
  ### ReplicatedMergeTree в базе данных atomic
</div>

Для таблиц [`ReplicatedMergeTree`](/ru/engines/table-engines/mergetree-family/replication) рекомендуется не указывать в параметрах движка путь в ZooKeeper и имя реплики. В этом случае будут использоваться параметры конфигурации [`default_replica_path`](../../operations/server-configuration-parameters/settings.md#default_replica_path) и [`default_replica_name`](../../operations/server-configuration-parameters/settings.md#default_replica_name). Если вы хотите задать параметры движка явно, рекомендуется использовать макрос `{uuid}`. Это гарантирует автоматическое создание уникальных путей в ZooKeeper для каждой таблицы.

<div id="metadata-disk">
  ### Диск для метаданных
</div>

Если в `SETTINGS` указан `disk`, этот диск используется для хранения файлов метаданных таблицы.
Например:

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```

Если не указано иное, по умолчанию используется диск, заданный в `database_disk.disk`.

<div id="see-also">
  ## См. также
</div>

* системная таблица [system.databases](../../operations/system-tables/databases.md)