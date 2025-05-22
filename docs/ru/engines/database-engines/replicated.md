---
slug: /ru/engines/database-engines/replicated
sidebar_position: 36
sidebar_label: Replicated
---

# [экспериментальный] Replicated {#replicated}

Движок основан на движке [Atomic](../../engines/database-engines/atomic.md). Он поддерживает репликацию метаданных через журнал DDL, записываемый в ZooKeeper и выполняемый на всех репликах для данной базы данных.

На одном сервере ClickHouse может одновременно работать и обновляться несколько реплицированных баз данных. Но не может существовать нескольких реплик одной и той же реплицированной базы данных.

## Создание базы данных {#creating-a-database}
``` sql
CREATE DATABASE testdb ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Параметры движка**

-   `zoo_path` — путь в ZooKeeper. Один и тот же путь ZooKeeper соответствует одной и той же базе данных.
-   `shard_name` — Имя шарда. Реплики базы данных группируются в шарды по имени.
-   `replica_name` — Имя реплики. Имена реплик должны быть разными для всех реплик одного и того же шарда.

:::note Предупреждение
Для таблиц [ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication) если аргументы не заданы, то используются аргументы по умолчанию: `/clickhouse/tables/{uuid}/{shard}` и `{replica}`. Они могут быть изменены в серверных настройках: [default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path) и [default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name). Макрос `{uuid}` раскрывается в `UUID` таблицы, `{shard}` и `{replica}` — в значения из конфига сервера. В будущем появится возможность использовать значения `shard_name` и `replica_name` аргументов движка базы данных `Replicated`.
:::
## Особенности и рекомендации {#specifics-and-recommendations}

DDL-запросы с базой данных `Replicated` работают похожим образом на [ON CLUSTER](../../sql-reference/distributed-ddl.md) запросы, но с небольшими отличиями.

Сначала DDL-запрос пытается выполниться на инициаторе (том хосте, который изначально получил запрос от пользователя). Если запрос не выполнился, то пользователь сразу получает ошибку, другие хосты не пытаются его выполнить. Если запрос успешно выполнился на инициаторе, то все остальные хосты будут автоматически делать попытки выполнить его.
Инициатор попытается дождаться выполнения запроса на других хостах (не дольше [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) и вернёт таблицу со статусами выполнения запроса на каждом хосте.

Поведение в случае ошибок регулируется настройкой [distributed_ddl_output_mode](../../operations/settings/settings.md#distributed_ddl_output_mode), для `Replicated` лучше выставлять её в `null_status_on_timeout` — т.е. если какие-то хосты не успели выполнить запрос за [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), то вместо исключения для них будет показан статус `NULL` в таблице.

В системной таблице [system.clusters](../../operations/system-tables/clusters.md) есть кластер с именем, как у реплицируемой базы, который состоит из всех реплик базы. Этот кластер обновляется автоматически при создании/удалении реплик, и его можно использовать для [Distributed](../../engines/table-engines/special/distributed.md#distributed) таблиц.

При создании новой реплики базы, эта реплика сама создаёт таблицы. Если реплика долго была недоступна и отстала от лога репликации — она сверяет свои локальные метаданные с актуальными метаданными в ZooKeeper, перекладывает лишние таблицы с данными в отдельную нереплицируемую базу (чтобы случайно не удалить что-нибудь лишнее), создаёт недостающие таблицы, обновляет имена таблиц, если были переименования. Данные реплицируются на уровне `ReplicatedMergeTree`, т.е. если таблица не реплицируемая, то данные реплицироваться не будут (база отвечает только за метаданные).

Запросы [`ALTER TABLE ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) допустимы, но не реплицируются. Движок базы данных может только добавить/извлечь/удалить партицию или кусок нынешней реплики. Однако если сама таблица использует движок реплицируемой таблицы, тогда данные будут реплицированы после применения `ATTACH`.

## Примеры использования {#usage-example}

Создадим реплицируемую базу на трех хостах:

``` sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

Выполним DDL-запрос на одном из хостов:

``` sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

Запрос выполнится на всех остальных хостах:

``` text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

Кластер в системной таблице `system.clusters`:

``` sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

``` text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

Создадим распределенную таблицу и вставим в нее данные:

``` sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

``` text
┌─hosts─┬─groupArray(n)─┐
│ node1 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

Добавление реплики:

``` sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

Новая реплика автоматически создаст все таблицы, которые есть в базе, а старые реплики перезагрузят из ZooKeeper-а конфигурацию кластера:

``` text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

Распределенная таблица также получит данные от нового хоста:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```