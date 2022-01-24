---
toc_priority: 36
toc_title: SYSTEM
---

# Запросы SYSTEM {#query-language-system}

-   [RELOAD EMBEDDED DICTIONARIES](#query_language-system-reload-emdedded-dictionaries)
-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [RELOAD MODELS](#query_language-system-reload-models)
-   [RELOAD MODEL](#query_language-system-reload-model)
-   [RELOAD FUNCTIONS](#query_language-system-reload-functions)
-   [RELOAD FUNCTION](#query_language-system-reload-functions)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [DROP UNCOMPRESSED CACHE](#query_language-system-drop-uncompressed-cache)
-   [DROP COMPILED EXPRESSION CACHE](#query_language-system-drop-compiled-expression-cache)
-   [DROP REPLICA](#query_language-system-drop-replica)
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
-   [RESTORE REPLICA](#query_language-system-restore-replica)
-   [RESTART REPLICAS](#query_language-system-restart-replicas)

## RELOAD EMBEDDED DICTIONARIES] {#query_language-system-reload-emdedded-dictionaries}
Перегружает все [Встроенные словари](../dictionaries/internal-dicts.md).
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

## RELOAD MODELS {#query_language-system-reload-models}

Перегружает все модели [CatBoost](../../guides/apply-catboost-model.md#applying-catboost-model-in-clickhouse), если их конфигурация была обновлена, без перезагрузки сервера.

**Синтаксис**

```sql
SYSTEM RELOAD MODELS
```

## RELOAD MODEL {#query_language-system-reload-model}

Полностью перегружает модель [CatBoost](../../guides/apply-catboost-model.md#applying-catboost-model-in-clickhouse) `model_name`, если ее конфигурация была обновлена, без перезагрузки сервера.

**Синтаксис**

```sql
SYSTEM RELOAD MODEL <model_name>
```

## RELOAD FUNCTIONS {#query_language-system-reload-functions}

Перезагружает все зарегистрированные [исполняемые пользовательские функции](../functions/index.md#executable-user-defined-functions) или одну из них из файла конфигурации.

**Синтаксис**

```sql
RELOAD FUNCTIONS
RELOAD FUNCTION function_name
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

Сбрасывает внутренний DNS кеш ClickHouse. Иногда (для старых версий ClickHouse) необходимо использовать эту команду при изменении инфраструктуры (смене IP адреса у другого ClickHouse сервера или сервера, используемого словарями).

Для более удобного (автоматического) управления кешем см. параметры disable_internal_dns_cache, dns_cache_update_period.

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

Сбрасывает кеш «засечек» (`mark cache`). Используется при разработке ClickHouse и тестах производительности.

## DROP REPLICA {#query_language-system-drop-replica}

Мертвые реплики можно удалить, используя следующий синтаксис:

``` sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

Удаляет путь реплики из ZooKeeper-а. Это полезно, когда реплика мертва и ее метаданные не могут быть удалены из ZooKeeper с помощью `DROP TABLE`, потому что такой таблицы больше нет. `DROP REPLICA` может удалить только неактивную / устаревшую реплику и не может удалить локальную реплику, используйте для этого `DROP TABLE`. `DROP REPLICA` не удаляет таблицы и не удаляет данные или метаданные с диска.

Первая команда удаляет метаданные реплики `'replica_name'` для таблицы `database.table`.
Вторая команда удаляет метаданные реплики `'replica_name'` для всех таблиц базы данных `database`.
Третья команда удаляет метаданные реплики `'replica_name'` для всех таблиц, существующих на локальном сервере (список таблиц генерируется из локальной реплики).
Четверая команда полезна для удаления метаданных мертвой реплики когда все другие реплики таблицы уже были удалены ранее, поэтому необходимо явно указать ZooKeeper путь таблицы. ZooKeeper путь это первый аргумент для `ReplicatedMergeTree` движка при создании таблицы.

## DROP UNCOMPRESSED CACHE {#query_language-system-drop-uncompressed-cache}

Сбрасывает кеш не сжатых данных. Используется при разработке ClickHouse и тестах производительности.
Для управления кешем не сжатых данных используйте следующие настройки уровня сервера [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) и настройки уровня запрос/пользователь/профиль [use_uncompressed_cache](../../operations/settings/settings.md#setting-use_uncompressed_cache)


## DROP COMPILED EXPRESSION CACHE {#query_language-system-drop-compiled-expression-cache}
Сбрасывает кеш скомпилированных выражений. Используется при разработке ClickHouse и тестах производительности.
Cкомпилированные выражения используются когда включена настройка уровня запрос/пользователь/профиль [compile-expressions](../../operations/settings/settings.md#compile-expressions)

## FLUSH LOGS {#query_language-system-flush_logs}

Записывает буферы логов в системные таблицы (например system.query_log). Позволяет не ждать 7.5 секунд при отладке.
Если буфер логов пустой, то этот запрос просто создаст системные таблицы.

## RELOAD CONFIG {#query_language-system-reload-config}

Перечитывает конфигурацию настроек ClickHouse. Используется при хранении конфигурации в zookeeper.

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
SYSTEM START MOVES [[db.]merge_tree_family_table_name]
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

После выполнения этого запроса таблица `[db.]replicated_merge_tree_family_table_name` синхронизирует команды из общего реплицированного лога в свою собственную очередь репликации. Затем запрос ждет, пока реплика не обработает все синхронизированные команды.

### RESTART REPLICA {#query_language-system-restart-replica}

Реинициализирует состояние сессий Zookeeper для таблицы семейства `ReplicatedMergeTree`. Сравнивает текущее состояние с состоянием в Zookeeper (как с эталоном) и при необходимости добавляет задачи в очередь репликации в Zookeeper. 
Инициализация очереди репликации на основе данных ZooKeeper происходит так же, как при `ATTACH TABLE`. Некоторое время таблица будет недоступна для любых операций.

``` sql
SYSTEM RESTART REPLICA [db.]replicated_merge_tree_family_table_name
```

### RESTORE REPLICA {#query_language-system-restore-replica}

Восстанавливает реплику, если метаданные в Zookeeper потеряны, но сами данные возможно существуют.

Работает только с таблицами семейства `ReplicatedMergeTree` и только если таблица находится в readonly-режиме.

Запрос можно выполнить если:

  - потерян корневой путь ZooKeeper `/`;
  - потерян путь реплик `/replicas`;
  - потерян путь конкретной реплики `/replicas/replica_name/`.

К реплике прикрепляются локально найденные куски, информация о них отправляется в Zookeeper.
Если присутствующие в реплике до потери метаданных данные не устарели, они не скачиваются повторно с других реплик. Поэтому восстановление реплики не означает повторную загрузку всех данных по сети.

!!! warning "Предупреждение"
    Потерянные данные в любых состояниях перемещаются в папку `detached/`. Куски, активные до потери данных (находившиеся в состоянии Committed), прикрепляются.

**Синтаксис**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

Альтернативный синтаксис:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**Пример**

Создание таблицы на нескольких серверах. После потери корневого пути реплики таблица будет прикреплена только для чтения, так как метаданные отсутствуют. Последний запрос необходимо выполнить на каждой реплике.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

Альтернативный способ:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

### RESTART REPLICAS {#query_language-system-restart-replicas}

Реинициализация состояния ZooKeeper-сессий для всех `ReplicatedMergeTree` таблиц. Сравнивает текущее состояние реплики с тем, что хранится в ZooKeeper, как c источником правды, и добавляет задачи в очередь репликации в ZooKeeper, если необходимо.
