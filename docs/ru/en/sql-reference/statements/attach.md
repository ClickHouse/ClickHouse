---
description: 'Документация по ATTACH'
sidebar_label: 'ATTACH'
sidebar_position: 40
slug: /sql-reference/statements/attach
title: 'Оператор ATTACH'
doc_type: 'reference'
---

Присоединяет таблицу или словарь, например при переносе базы данных на другой сервер.

**Синтаксис**

```sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

Запрос не создаёт данные на диске, а предполагает, что они уже находятся в нужных местах, и лишь добавляет на сервер информацию об указанной таблице, словаре или базе данных. После выполнения запроса `ATTACH` сервер будет знать о существовании таблицы, словаря или базы данных.

Если таблица ранее была отсоединена (запрос [DETACH](../../sql-reference/statements/detach.md)), то есть её структура уже известна, вы можете использовать сокращённую форму без определения структуры.

<div id="attach-existing-table">
  ## Присоединение существующей таблицы
</div>

**Синтаксис**

```sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

Этот запрос используется при запуске сервера. Сервер хранит метаданные таблиц в виде файлов с запросами `ATTACH`, которые просто выполняются при запуске (за исключением некоторых системных таблиц, которые явно создаются на сервере).

Если таблица была отсоединена навсегда, при запуске сервера она не будет присоединена повторно, поэтому нужно явно выполнить запрос `ATTACH`.

<div id="create-new-table-and-attach-data">
  ## Создание новой таблицы и присоединение данных
</div>

<div id="with-specified-path-to-table-data">
  ### С указанным путём к данным таблицы
</div>

Этот запрос создаёт новую таблицу с указанной структурой и присоединяет к ней данные таблицы из указанного каталога в `user_files`.

**Синтаксис**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**Пример**

```sql title="Query"
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```

```sql title="Response"
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

<div id="with-specified-table-uuid">
  ### С указанным UUID таблицы
</div>

Этот запрос создаёт новую таблицу с указанной структурой и присоединяет данные из таблицы с указанным UUID.
Поддерживается движком базы данных [Atomic](../../engines/database-engines/atomic.md).

**Синтаксис**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

<div id="attach-mergetree-table-as-replicatedmergetree">
  ## Присоединение таблицы семейства MergeTree как ReplicatedMergeTree
</div>

Позволяет присоединить нереплицированную таблицу семейства MergeTree как ReplicatedMergeTree. Таблица ReplicatedMergeTree будет создана со значениями настроек `default_replica_path` и `default_replica_name`. Также можно присоединить реплицированную таблицу как обычную таблицу семейства MergeTree.

Обратите внимание, что данные таблицы в ZooKeeper этим запросом не затрагиваются. Это означает, что после присоединения вам нужно добавить метаданные в ZooKeeper с помощью `SYSTEM RESTORE REPLICA` или очистить их с помощью `SYSTEM DROP REPLICA ... FROM ZKPATH ...`.

Если вы пытаетесь добавить реплику в существующую таблицу ReplicatedMergeTree, имейте в виду, что все локальные данные преобразованной таблицы семейства MergeTree будут переведены в состояние detached.

**Синтаксис**

```sql
ATTACH TABLE [db.]name AS [NOT] REPLICATED
```

**Преобразовать таблицу в Replicated**

```sql
DETACH TABLE test;
ATTACH TABLE test AS REPLICATED;
SYSTEM RESTORE REPLICA test;
```

**Преобразовать таблицу в нереплицируемую**

Получите путь ZooKeeper и имя реплики таблицы:

```sql title="Query"
SELECT replica_name, zookeeper_path FROM system.replicas WHERE table='test';
```

```sql title="Response"
┌─replica_name─┬─zookeeper_path─────────────────────────────────────────────┐
│ r1           │ /clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1 │
└──────────────┴────────────────────────────────────────────────────────────┘
```

Присоедините таблицу без репликации и удалите данные реплики из ZooKeeper:

```sql title="Query"
DETACH TABLE test;
ATTACH TABLE test AS NOT REPLICATED;
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1';
```

<div id="attach-existing-dictionary">
  ## Присоединить существующий словарь
</div>

Присоединяет ранее отсоединённый словарь.

**Синтаксис**

```sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

<div id="attach-existing-database">
  ## Присоединить существующую базу данных
</div>

Присоединяет ранее отсоединённую базу данных.

**Синтаксис**

```sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```