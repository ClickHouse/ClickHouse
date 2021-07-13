
# [экспериментальный] Replicated {#replicated}

Движок основан на движке [Atomic](../../engines/database-engines/atomic.md). Он поддерживает репликацию метаданных через журнал DDL, записываемый в ZooKeeper и выполняемый на всех репликах для данной базы данных.

На одном сервере Clickhouse может одновременно работать и обновляться несколько реплицированных баз данных. Но не может существовать нескольких реплик одной и той же реплицированной базы данных.

## Создание базы данных {#creating-a-database}
``` sql
    CREATE DATABASE testdb ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Параметры движка**

-   `zoo_path` — путь в ZooKeeper. Один и тот же путь ZooKeeper соответствует одной и той же базе данных.
-   `shard_name` — Имя шарда. Реплики базы данных группируются в шарды по имени.
-   `replica_name` — Имя реплики. Имена реплик должны быть разными для всех реплик одного и того же шарда.

!!! note "Предупреждение"
    Для таблиц [ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication) если аргументы не заданы, то используются аргументы по умолчанию: `/clickhouse/tables/{uuid}/{shard}` и `{replica}`. Они могут быть изменены в серверных настройках: [default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path) и [default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name). Макрос `{uuid}` раскрывается в `UUID` таблицы, `{shard}` и `{replica}` — в значения из конфига сервера. В будущем появится возможность использовать значения `shard_name` и `replica_name` аргументов движка базы данных `Replicated`.

## Особенности и рекомендации {#specifics-and-recommendations}

DDL-запросы с базой данных `Replicated` работают похожим образом на [ON CLUSTER](../../sql-reference/distributed-ddl.md) запросы, но с небольшими отличиями. 

Сначала DDL-запрос пытается выполниться на инициаторе (том хосте, который изначально получил запрос от пользователя). Если запрос не выполнился, то пользователь сразу получает ошибку, другие хосты не пытаются его выполнить. Если запрос успешно выполнился на инициаторе, то все остальные хосты будут автоматически делать попытки выполнить его. 
Инициатор попытается дождаться выполнения запроса на других хостах (не дольше [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) и вернёт таблицу со статусами выполнения запроса на каждом хосте. 

Поведение в случае ошибок регулируется настройкой [distributed_ddl_output_mode](../../operations/settings/settings.md#distributed_ddl_output_mode), для `Replicated` лучше выставлять её в `null_status_on_timeout` — т.е. если какие-то хосты не успели выполнить запрос за [distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), то вместо исключения для них будет показан статус `NULL` в таблице.

В системной таблице [system.clusters](../../operations/system-tables/clusters.md) есть кластер с именем, как у реплицируемой базы, который состоит из всех реплик базы. Этот кластер обновляется автоматически при создании/удалении реплик, и его можно использовать для [Distributed](../../engines/table-engines/special/distributed.md#distributed) таблиц.

 При создании новой реплики базы, эта реплика сама создаёт таблицы. Если реплика долго была недоступна и отстала от лога репликации — она сверяет свои локальные метаданные с актуальными метаданными в ZooKeeper, перекладывает лишние таблицы с данными в отдельную нереплицируемую базу (чтобы случайно не удалить что-нибудь лишнее), создаёт недостающие таблицы, обновляет имена таблиц, если были переименования. Данные реплицируются на уровне `ReplicatedMergeTree`, т.е. если таблица не реплицируемая, то данные реплицироваться не будут (база отвечает только за метаданные).

## Примеры использования {#usage-example}

The example must show usage and use cases. The following text contains the recommended parts of this section.

Input table:

``` text
```

Query:

``` sql
```

Result:

``` text
```

Follow up with any text to clarify the example.

**See Also** 

-   [link](#)
