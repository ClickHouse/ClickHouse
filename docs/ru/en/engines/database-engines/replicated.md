---
description: 'Этот движок основан на движке Atomic. Он поддерживает репликацию
  метаданных через журнал DDL, который записывается в ZooKeeper и выполняется
  на всех репликах для конкретной базы данных.'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'reference'
---

Этот движок основан на движке [Atomic](../../engines/database-engines/atomic.md). Он поддерживает репликацию метаданных через журнал DDL, который записывается в ZooKeeper и выполняется на всех репликах для конкретной базы данных.

На одном сервере ClickHouse могут одновременно работать и обновляться несколько баз данных Replicated. Но у одной и той же базы данных Replicated не может быть нескольких реплик.

<div id="creating-a-database">
  ## Создание базы данных
</div>

```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Параметры движка**

* `zoo_path` — путь ZooKeeper. Одному и тому же пути ZooKeeper соответствует одна и та же база данных.
* `shard_name` — имя сегмента. Реплики базы данных группируются в сегменты по `shard_name`.
* `replica_name` — имя реплики. Имена реплик должны различаться у всех реплик в пределах одного сегмента.

Параметры можно не указывать; в этом случае отсутствующие параметры будут заменены значениями по умолчанию.

Если `zoo_path` содержит макрос `{uuid}`, необходимо явно указать UUID или добавить [предложение ON CLUSTER](../../sql-reference/distributed-ddl.md) в оператор CREATE, чтобы все реплики использовали один и тот же UUID для этой базы данных.

Для таблиц [ReplicatedMergeTree](/ru/engines/table-engines/mergetree-family/replication), если аргументы не указаны, используются значения по умолчанию: `/clickhouse/tables/{uuid}/{shard}` и `{replica}`. Их можно изменить в настройках сервера [default&#95;replica&#95;path](../../operations/server-configuration-parameters/settings.md#default_replica_path) и [default&#95;replica&#95;name](../../operations/server-configuration-parameters/settings.md#default_replica_name). Макрос `{uuid}` разворачивается в UUID таблицы, а `{shard}` и `{replica}` — в значения из конфигурации сервера, а не из аргументов движка базы данных. Однако в будущем можно будет использовать `shard_name` и `replica_name` базы данных Replicated.

Также поддерживается использование вспомогательного кластера ZooKeeper для хранения метаданных базы данных Replicated вместо кластера ZooKeeper по умолчанию. Базу данных Replicated со вспомогательным кластером ZooKeeper можно создать с помощью SQL следующим образом:

```sql
CREATE DATABASE database_name ENGINE = Replicated('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'shard_name', 'replica_name')
```

<div id="specifics-and-recommendations">
  ## Особенности и рекомендации
</div>

DDL-запросы в базе данных `Replicated` работают аналогично запросам [ON CLUSTER](../../sql-reference/distributed-ddl.md), но с небольшими отличиями.

Сначала DDL-запрос пытается выполниться на инициаторе (хосте, который изначально получил запрос от пользователя). Если запрос не выполняется, пользователь сразу получает ошибку, а другие хосты не пытаются его выполнить. Если запрос успешно выполнен на инициаторе, все остальные хосты будут автоматически повторять попытки, пока тоже не выполнят его. Инициатор попытается дождаться завершения запроса на других хостах (не дольше [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) и вернет таблицу со статусами выполнения запроса на каждом хосте.

Поведение в случае ошибок регулируется настройкой [distributed&#95;ddl&#95;output&#95;mode](../../operations/settings/settings.md#distributed_ddl_output_mode); для базы данных `Replicated` лучше установить значение `null_status_on_timeout` — то есть если какие-то хосты не успели выполнить запрос за время [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), не нужно генерировать исключение, а вместо этого следует показать для них статус `NULL` в таблице.

Системная таблица [system.clusters](../../operations/system-tables/clusters.md) содержит кластер с именем, совпадающим с именем реплицируемой базы данных; он состоит из всех реплик этой базы данных. Этот кластер автоматически обновляется при создании и удалении реплик и может использоваться для таблиц [Distributed](/ru/engines/table-engines/special/distributed).

При создании новой реплики базы данных эта реплика сама создает таблицы. Если реплика была недоступна долгое время и отстала от лога репликации, она сверяет свои локальные метаданные с текущими метаданными в ZooKeeper, перемещает лишние таблицы с данными в отдельную нереплицируемую базу данных (чтобы случайно не удалить ничего лишнего), создает отсутствующие таблицы и обновляет их имена, если они были переименованы. Данные реплицируются на уровне `ReplicatedMergeTree`, то есть если таблица не реплицируется, данные тоже реплицироваться не будут (база данных отвечает только за метаданные).

Запросы [`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) разрешены, но не реплицируются. Движок базы данных будет только добавлять/получать/удалять партицию/часть у текущей реплики. Однако если сама таблица использует движок таблицы Replicated, то после использования `ATTACH` данные будут реплицированы.

Если вам нужно только настроить кластер без поддержки репликации таблиц, воспользуйтесь возможностью [Cluster Discovery](../../operations/cluster-discovery.md).

<div id="usage-example">
  ## Пример использования
</div>

Создание кластера из трёх узлов:

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

Создание базы данных в кластере с неявно заданными параметрами:

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

Выполнение DDL-запроса:

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

Отображение системной таблицы:

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

Создание distributed таблицы и вставка данных:

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

Добавление реплики на ещё один хост:

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

Добавление реплики на ещё один хост, если в `zoo_path` используется макрос `{uuid}`:

```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

Конфигурация кластера будет выглядеть следующим образом:

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

distributed таблица также будет получать данные с нового хоста:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

<div id="settings">
  ## Настройки
</div>

Поддерживаются следующие настройки:

| Настройка                                                                    | По умолчанию                   | Описание                                                                                                                                                                                                                                                                                                                                          |
| ---------------------------------------------------------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_broken_tables_ratio`                                                    | 1                              | Не восстанавливать реплику автоматически, если доля устаревших таблиц среди всех таблиц больше                                                                                                                                                                                                                                                    |
| `max_replication_lag_to_enqueue`                                             | 50                             | Реплика сгенерирует исключение при попытке выполнить запрос, если её задержка репликации больше                                                                                                                                                                                                                                                   |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | Реплики попытаются отменить запрос, если тайм-аут превышен, но хост-инициатор ещё не выполнил его                                                                                                                                                                                                                                                 |
| `collection_name`                                                            |                                | Имя коллекции, определённой в конфигурации сервера, где указана вся информация для аутентификации кластера                                                                                                                                                                                                                                        |
| `check_consistency`                                                          | true                           | Проверять согласованность локальных метаданных и метаданных в Keeper, выполнять восстановление реплики при несогласованности                                                                                                                                                                                                                      |
| `max_retries_before_automatic_recovery`                                      | 10                             | Максимальное число попыток выполнить элемент очереди перед тем, как пометить реплику как потерянную и восстановить её из снимка (0 означает бесконечное число попыток)                                                                                                                                                                            |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | Если включено, при обработке DDL-запросов в базах данных Replicated пропускаются создание и обмен DDL-запросами для временных таблиц refreshable materialized view, если это возможно                                                                                                                                                             |
| `logs_to_keep`                                                               | 1000                           | Число журналов, сохраняемых в ZooKeeper по умолчанию для базы данных Replicated.                                                                                                                                                                                                                                                                  |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | Путь к базе данных в ZooKeeper. Используется при создании базы данных, если аргументы опущены.                                                                                                                                                                                                                                                    |
| `default_replica_shard_name`                                                 | `{shard}`                      | Имя сегмента реплики в базе данных. Используется при создании базы данных, если аргументы опущены.                                                                                                                                                                                                                                                |
| `default_replica_name`                                                       | `{replica}`                    | Имя реплики в базе данных. Используется при создании базы данных, если аргументы опущены.                                                                                                                                                                                                                                                         |
| `internal_replication`                                                       | false                          | Определяет, будет ли таблица Distributed, созданная с кластером этой базы данных Replicated, отправлять данные на одну из реплик (внутренняя репликация означает, что реплики кластера выполняют репликацию самостоятельно) или на все реплики (без внутренней репликации таблица Distributed будет отправлять вставленные данные на все реплики) |

Значения по умолчанию могут быть переопределены в файле конфигурации

```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
        <internal_replication>false</internal_replication>
    </database_replicated>
</clickhouse>
```