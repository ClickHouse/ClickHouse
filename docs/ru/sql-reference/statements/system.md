---
toc_priority: 36
toc_title: SYSTEM
---

# Запросы SYSTEM {#query-language-system}

-   [RELOAD EMBEDDED DICTIONARIES](#query_language-system-reload-emdedded-dictionaries) 
-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [DROP UNCOMPRESSED CACHE](#query_language-system-drop-uncompressed-cache) 
-   [DROP COMPILED EXPRESSION CACHE](#query_language-system-drop-compiled-expression-cache)
-   [FLUSH LOGS](#query_language-system-flush_logs)
-   [RELOAD CONFIG](#query_language-system-reload-config)
-   [SHUTDOWN](#query_language-system-shutdown)
-   [KILL](#query_language-system-kill)
-   [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
-   [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)
-   [STOP MERGES](#query_language-system-stop-merges)
-   [START MERGES](#query_language-system-start-merges)
-   [STOP TTL MERGES](#query_language-stop-ttl-merges) 
-   [START TTL MERGES](#query_language-start-ttl-merges) 
-   [STOP MOVES](#query_language-stop-moves) 
-   [START MOVES](#query_language-start-moves) 
-   [STOP FETCHES](#query_language-system-stop-fetches)
-   [START FETCHES](#query_language-system-start-fetches)
-   [STOP REPLICATED SENDS](#query_language-system-start-replicated-sends)
-   [START REPLICATED SENDS](#query_language-system-start-replicated-sends)
-   [STOP REPLICATION QUEUES](#query_language-system-stop-replication-queues)
-   [START REPLICATION QUEUES](#query_language-system-start-replication-queues)
-   [SYNC REPLICA](#query_language-system-sync-replica)
-   [RESTART REPLICA](#query_language-system-restart-replica)
-   [RESTART REPLICAS](#query_language-system-restart-replicas) 

## RELOAD EMBEDDED DICTIONARIES] {#query_language-system-reload-emdedded-dictionaries} 
Перегружет все [Встроенные словари](../dictionaries/internal-dicts.md).
По умолчанию встроенные словари выключены. 
Всегда возвращает `Ok.`, вне зависимости от результата обновления встроенных словарей.
   
## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

Перегружает все словари, которые были успешно загружены до этого.
По умолчанию включена ленивая загрузка [dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load), поэтому словари не загружаются автоматически при старте, а только при первом обращении через dictGet или SELECT к ENGINE=Dictionary. После этого такие словари (LOADED) будут перегружаться командой `system reload dictionaries`.
Всегда возвращает `Ok.`, вне зависимости от результата обновления словарей.

## RELOAD DICTIONARY Dictionary_name {#query_language-system-reload-dictionary}

Полностью перегружает словарь `dictionary_name`, вне зависимости от состояния словаря (LOADED/NOT_LOADED/FAILED).
Всегда возвращает `Ok.`, вне зависимости от результата обновления словаря.
Состояние словаря можно проверить запросом к `system.dictionaries`.

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

Сбрасывает внутренний DNS кеш ClickHouse. Иногда (для старых версий ClickHouse) необходимо использовать эту команду при изменении инфраструктуры (смене IP адреса у другого ClickHouse сервера или сервера, используемого словарями).

Для более удобного (автоматического) управления кешем см. параметры disable_internal_dns_cache, dns_cache_update_period.

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

Сбрасывает кеш «засечек» (`mark cache`). Используется при разработке ClickHouse и тестах производительности.

## DROP UNCOMPRESSED CACHE {#query_language-system-drop-uncompressed-cache}

Сбрасывает кеш не сжатых данных. Используется при разработке ClickHouse и тестах производительности.
Для управления кешем не сжатых данных используйте следующие настройки уровня сервера [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) и настройки уровня запрос/пользователь/профиль [use_uncompressed_cache](../../operations/settings/settings.md#setting-use_uncompressed_cache)


## DROP COMPILED EXPRESSION CACHE {#query_language-system-drop-compiled-expression-cache}
Сбрасывает кеш скомпилированных выражений. Используется при разработке ClickHouse и тестах производительности.
Компилированные выражения используются когда включена настройка уровня запрос/пользователь/профиль [compile](../../operations/settings/settings.md#compile)

## FLUSH LOGS {#query_language-system-flush_logs}

Записывает буферы логов в системные таблицы (например system.query_log). Позволяет не ждать 7.5 секунд при отладке.
Если буфер логов пустой, то этот запрос просто создаст системные таблицы.

## RELOAD CONFIG {#query_language-system-reload-config}

Перечитывает конфигурацию настроек ClickHouse. Используется при хранении конфигурации в zookeeeper.

## SHUTDOWN {#query_language-system-shutdown}

Штатно завершает работу ClickHouse (аналог `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

Аварийно завершает работу ClickHouse (аналог `kill -9 {$pid_clickhouse-server}`)

## Управление распределёнными таблицами {#query-language-system-distributed}

ClickHouse может оперировать [распределёнными](../../sql-reference/statements/system.md) таблицами. Когда пользователь вставляет данные в эти таблицы, ClickHouse сначала формирует очередь из данных, которые должны быть отправлены на узлы кластера, а затем асинхронно отправляет подготовленные данные. Вы можете управлять очередью с помощью запросов [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) и [FLUSH DISTRIBUTED](#query_language-system-flush-distributed). Также есть возможность синхронно вставлять распределенные данные с помощью настройки [insert_distributed_sync](../../operations/settings/settings.md#insert_distributed_sync).

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Отключает фоновую отправку при вставке данных в распределённые таблицы.

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

В синхронном режиме отправляет все данные на узлы кластера. Если какие-либо узлы недоступны, ClickHouse генерирует исключение и останавливает выполнение запроса. Такой запрос можно повторять до успешного завершения, что будет означать возвращение связанности с остальными узлами кластера.

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Включает фоновую отправку при вставке данных в распределенные таблицы.

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

## Managing MergeTree Tables {#query-language-system-mergetree}

ClickHouse может управлять фоновыми процессами в [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) таблицах.

### STOP MERGES {#query_language-system-stop-merges}

Позволяет остановить фоновые мержи для таблиц семейства MergeTree:

``` sql
SYSTEM STOP MERGES [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

!!! note "Note"
    `DETACH / ATTACH` таблицы восстанавливает фоновые мержи для этой таблицы (даже в случае отключения фоновых мержей для всех таблиц семейства MergeTree до `DETACH`).

### START MERGES {#query_language-system-start-merges}

Включает фоновые мержи для таблиц семейства MergeTree:

``` sql
SYSTEM START MERGES [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

### STOP TTL MERGES {#query_language-stop-ttl-merges}

Позволяет остановить фоновые процессы удаления старых данных основанные на [выражениях TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) для таблиц семейства MergeTree:
Возвращает `Ok.` даже если указана несуществующая таблица или таблица имеет тип отличный от MergeTree. Возвращает ошибку если указана не существующая база данных:

``` sql
SYSTEM STOP TTL MERGES [[db.]merge_tree_family_table_name]
```

### START TTL MERGES {#query_language-start-ttl-merges} 

Запускает фоновые процессы удаления старых данных основанные на [выражениях TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) для таблиц семейства MergeTree:
Возвращает `Ok.` даже если указана несуществующая таблица или таблица имеет тип отличный от MergeTree. Возвращает ошибку если указана не существующая база данных:

``` sql
SYSTEM START TTL MERGES [[db.]merge_tree_family_table_name]
```

### STOP MOVES {#query_language-stop-moves} 

Позволяет остановить фоновые процессы переноса данных основанные [табличных выражениях TTL с использованием TO VOLUME или TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) for tables in the MergeTree family:
Возвращает `Ok.` даже если указана несуществующая таблица или таблица имеет тип отличный от MergeTree. Возвращает ошибку если указана не существующая база данных:

``` sql
SYSTEM STOP MOVES [[db.]merge_tree_family_table_name]
```

### START MOVES {#query_language-start-moves} 

Запускает фоновые процессы переноса данных основанные [табличных выражениях TTL с использованием TO VOLUME или TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) for tables in the MergeTree family:
Возвращает `Ok.` даже если указана несуществующая таблица или таблица имеет тип отличный от MergeTree. Возвращает ошибку если указана не существующая база данных:

``` sql
SYSTEM STOP MOVES [[db.]merge_tree_family_table_name]
```

## Managing ReplicatedMergeTree Tables {#query-language-system-replicated}

ClickHouse может управлять фоновыми процессами связанными c репликацией в таблицах семейства [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md).

### STOP FETCHES {#query_language-system-stop-fetches}
Позволяет остановить фоновые процессы синхронизации новыми вставленными кусками данных с другими репликами в кластере для таблиц семейства `ReplicatedMergeTree`:
Всегда возвращает `Ok.` вне зависимости от типа таблицы и даже если таблица или база данных не существет.

``` sql
SYSTEM STOP FETCHES [[db.]replicated_merge_tree_family_table_name]
```

### START FETCHES {#query_language-system-start-fetches}
Позволяет запустить фоновые процессы синхронизации новыми вставленными кусками данных с другими репликами в кластере для таблиц семейства `ReplicatedMergeTree`:
Всегда возвращает `Ok.` вне зависимости от типа таблицы и даже если таблица или база данных не существет.

``` sql
SYSTEM START FETCHES [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATED SENDS {#query_language-system-start-replicated-sends}
Позволяет остановить фоновые процессы отсылки новых вставленных кусков данных другим репликам в кластере для таблиц семейства `ReplicatedMergeTree`:

``` sql
SYSTEM STOP REPLICATED SENDS [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATED SENDS {#query_language-system-start-replicated-sends}
Позволяет запустить фоновые процессы отсылки новых вставленных кусков данных другим репликам в кластере для таблиц семейства `ReplicatedMergeTree`:

``` sql
SYSTEM START REPLICATED SENDS [[db.]replicated_merge_tree_family_table_name]
```

### STOP REPLICATION QUEUES {#query_language-system-stop-replication-queues}
Останавливает фоновые процессы разбора заданий из очереди репликации которая хранится в Zookeeper для таблиц семейства `ReplicatedMergeTree`. Возможные типы заданий - merges, fetches, mutation, DDL запросы с ON CLUSTER: 

``` sql
SYSTEM STOP REPLICATION QUEUES [[db.]replicated_merge_tree_family_table_name]
```

### START REPLICATION QUEUES {#query_language-system-start-replication-queues}
Запускает фоновые процессы разбора заданий из очереди репликации которая хранится в Zookeeper для таблиц семейства `ReplicatedMergeTree`. Возможные типы заданий - merges, fetches, mutation, DDL запросы с ON CLUSTER: 

``` sql
SYSTEM START REPLICATION QUEUES [[db.]replicated_merge_tree_family_table_name]
```

### SYNC REPLICA {#query_language-system-sync-replica}
Ждет когда таблица семейства `ReplicatedMergeTree` будет синхронизирована с другими репликами в кластере, будет работать до достижения `receive_timeout`, если синхронизация для таблицы отключена в настоящий момент времени:  

``` sql
SYSTEM SYNC REPLICA [db.]replicated_merge_tree_family_table_name
```

### RESTART REPLICA {#query_language-system-restart-replica}
Реинициализация состояния Zookeeper сессий для таблицы семейства `ReplicatedMergeTree`, сравнивает текущее состояние с тем что хранится в Zookeeper как источник правды и добавляет задачи Zookeeper очередь если необходимо  
Инициализация очереди репликации на основе данных ZooKeeper, происходит так же как при attach table. На короткое время таблица станет недоступной для любых операций.

``` sql
SYSTEM RESTART REPLICA [db.]replicated_merge_tree_family_table_name
```

### RESTART REPLICAS {#query_language-system-restart-replicas}
Реинициализация состояния Zookeeper сессий для всех `ReplicatedMergeTree` таблиц, сравнивает текущее состояние с тем что хранится в Zookeeper как источник правды и добавляет задачи Zookeeper очередь если необходимо

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/system/) <!--hide-->
