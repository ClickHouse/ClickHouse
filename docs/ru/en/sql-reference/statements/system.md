---
description: 'Документация по командам SYSTEM'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'Команды SYSTEM'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="system-statements">
  # Команды SYSTEM
</div>

<div id="reload-embedded-dictionaries">
  ## SYSTEM RELOAD EMBEDDED DICTIONARIES
</div>

Перезагружает все [внутренние словари](./create/dictionary/overview.md).
По умолчанию внутренние словари отключены.
Всегда возвращает `Ok.` независимо от результата обновления внутренних словарей.

<div id="reload-dictionaries">
  ## SYSTEM RELOAD DICTIONARIES
</div>

Запрос `SYSTEM RELOAD DICTIONARIES` перезагружает словари со статусом `LOADED` (см. столбец `status` таблицы [`system.dictionaries`](/ru/operations/system-tables/dictionaries)), то есть словари, которые ранее уже были успешно загружены.
По умолчанию словари загружаются отложенно (см. [dictionaries&#95;lazy&#95;load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load)), поэтому вместо автоматической загрузки при запуске они инициализируются при первом обращении — при вызове функции [`dictGet`](/ru/sql-reference/functions/ext-dict-functions#dictGet) или выполнении `SELECT` из таблиц с `ENGINE = Dictionary`.

**Синтаксис**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

<div id="reload-dictionary">
  ## SYSTEM RELOAD DICTIONARY
</div>

Полностью перезагружает словарь `dictionary_name` вне зависимости от его состояния (LOADED / NOT&#95;LOADED / FAILED).
Всегда возвращает `Ok.` независимо от результата обновления словаря.

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

Статус словаря можно проверить с помощью запроса к таблице `system.dictionaries`.

```sql
SELECT name, status FROM system.dictionaries;
```

<div id="reload-models">
  ## SYSTEM RELOAD MODELS
</div>

:::note
Этот оператор и `SYSTEM RELOAD MODEL` лишь выгружают модели CatBoost из clickhouse-library-bridge. Функция `catboostEvaluate()`
загружает модель при первом обращении, если она ещё не загружена.
:::

Выгружает все модели CatBoost.

**Синтаксис**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

<div id="reload-model">
  ## SYSTEM RELOAD MODEL
</div>

Выгружает модель CatBoost, расположенную по пути `model_path`.

**Синтаксис**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

<div id="reload-functions">
  ## SYSTEM RELOAD FUNCTIONS
</div>

Перезагружает все зарегистрированные [исполняемые пользовательские функции](/ru/sql-reference/functions/udf#executable-user-defined-functions) или одну из них из файла конфигурации.

**Синтаксис**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

<div id="reload-asynchronous-metrics">
  ## SYSTEM RELOAD ASYNCHRONOUS METRICS
</div>

Пересчитывает все [асинхронные метрики](../../operations/system-tables/asynchronous_metrics.md). Поскольку асинхронные метрики периодически обновляются в соответствии с настройкой [asynchronous&#95;metrics&#95;update&#95;period&#95;s](../../operations/server-configuration-parameters/settings.md), обычно нет необходимости обновлять их вручную с помощью этого оператора.

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

<div id="drop-dns-cache">
  ## SYSTEM CLEAR|DROP DNS CACHE
</div>

Очищает внутренний DNS-кэш ClickHouse. Иногда (в старых версиях ClickHouse) эту команду необходимо использовать при изменении инфраструктуры (например, при смене IP-адреса другого сервера ClickHouse или сервера, используемого словарями).

Для более удобного (автоматического) управления кэшем см. параметры `disable_internal_dns_cache`, `dns_cache_max_entries`, `dns_cache_update_period`.

<div id="drop-mark-cache">
  ## SYSTEM CLEAR|DROP MARK CACHE
</div>

Очищает кэш меток.

<div id="drop-primary-index-cache">
  ## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE
</div>

Очищает кэш первичного индекса, в котором в оперативной памяти хранятся первичные ключи таблиц [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md).
Его размер задаётся параметром уровня сервера [`primary_index_cache_size`](../../operations/server-configuration-parameters/settings.md#primary_index_cache_size).

<div id="drop-iceberg-metadata-cache">
  ## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE
</div>

Очищает кэш метаданных Iceberg.

<div id="drop-avro-schema-cache">
  ## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE
</div>

Очищает кэши Confluent Schema Registry для отдельных URL, используемые форматом `AvroConfluent`. При этом сбрасываются оба кэша: кэш получения схем (id → схема) и кэш регистрации схем (subject + схема → id), поэтому при последующих чтениях и записях система снова будет обращаться к registry-серверу. Это полезно, если схема была удалена или изменена на стороне registry, а также для проверки идемпотентности registry в тестах.

<div id="drop-parquet-metadata-cache">
  ## SYSTEM DROP PARQUET METADATA CACHE
</div>

Очищает кэш метаданных Parquet.

<div id="drop-point-in-polygon-cache">
  ## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE
</div>

Очищает кэш предварительно обработанных константных многоугольников, используемых функцией [`pointInPolygon`](../functions/geo/coordinates.md#pointinpolygon). Настроенное ограничение размера (настройка сервера `point_in_polygon_cache_size`) остаётся без изменений, поэтому после этого кэш продолжит принимать новые записи. Чтобы отключить кэш, установите `point_in_polygon_cache_size` в `0`.

<div id="drop-text-index-caches">
  ## SYSTEM CLEAR|DROP TEXT INDEX CACHES
</div>

Очищает кэши токенов, заголовка и postings текстового индекса.

Если нужно очистить один из этих кэшей по отдельности, можно выполнить

* `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
* `SYSTEM CLEAR TEXT INDEX HEADER CACHE`, или
* `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

<div id="drop-index-mark-cache">
  ## SYSTEM CLEAR|DROP INDEX MARK CACHE
</div>

Очищает кэш меток вторичных индексов для пропуска данных.

<div id="drop-index-uncompressed-cache">
  ## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE
</div>

Очищает кэш несжатых блоков для вторичных индексов пропуска данных.

<div id="drop-mmap-cache">
  ## SYSTEM CLEAR|DROP MMAP CACHE
</div>

Очищает кэш файлов, отображаемых в память.

<div id="drop-page-cache">
  ## SYSTEM CLEAR|DROP PAGE CACHE
</div>

Очищает кэш страниц в пространстве пользователя — собственный кэш ClickHouse в памяти для данных, считываемых из нижележащего хранилища.

<div id="drop-vector-similarity-index-cache">
  ## SYSTEM CLEAR|DROP VECTOR SIMILARITY INDEX CACHE
</div>

Очищает кэш индекса векторного сходства.

<div id="drop-connections-cache">
  ## SYSTEM CLEAR|DROP CONNECTIONS CACHE
</div>

Очищает кэш пулов HTTP-соединений, используемых для исходящих подключений.

<div id="drop-s3-client-cache">
  ## SYSTEM CLEAR|DROP S3 CLIENT CACHE
</div>

Очищает кэш S3-клиентов.

<div id="prewarm-mark-cache">
  ## SYSTEM PREWARM MARK CACHE
</div>

Загружает метки таблицы в [кэш меток](#drop-mark-cache). Метки вторичных индексов также загружаются в [кэш меток индекса](#drop-index-mark-cache).

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="prewarm-primary-index-cache">
  ## SYSTEM PREWARM PRIMARY INDEX CACHE
</div>

Загружает индексы первичного ключа таблицы `MergeTree` в [кэш первичного индекса](#drop-primary-index-cache).

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="drop-disk-metadata-cache">
  ## SYSTEM CLEAR|DROP DISK METADATA CACHE
</div>

Очищает кэш метаданных для указанного диска.

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

<div id="sync-filesystem-cache">
  ## SYSTEM SYNC FILESYSTEM CACHE
</div>

Сверяет хранящееся в памяти ClickHouse состояние файлового кэша с файлами кэша, фактически присутствующими на диске, и возвращает `cache_name`, `path` и загруженный `size` каждого кэшированного сегмента файла. При указании имени кэша операция ограничивается только этим кэшем.

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

<div id="drop-distributed-cache">
  ## SYSTEM CLEAR|DROP DISTRIBUTED CACHE
</div>

:::note
`SYSTEM CLEAR|DROP DISTRIBUTED CACHE` доступна только в ClickHouse Cloud.
:::

Удаляет распределённый кэш. Используйте `CONNECTIONS`, чтобы удалить только кэшированные соединения с серверами распределённого кэша, либо укажите идентификатор сервера, чтобы выбрать конкретный сервер.

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

<div id="drop-replica">
  ## SYSTEM DROP REPLICA
</div>

Неактивные реплики таблиц `ReplicatedMergeTree` можно удалить с помощью следующего синтаксиса:

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

Запросы удаляют путь реплики `ReplicatedMergeTree` в ZooKeeper. Это полезно, когда реплика вышла из строя и её метаданные нельзя удалить из ZooKeeper с помощью `DROP TABLE`, потому что такой таблицы больше не существует. Будет удалена только неактивная/устаревшая реплика; локальную реплику удалить нельзя, для этого используйте `DROP TABLE`. `DROP REPLICA` не удаляет никакие таблицы и не удаляет с диска ни данные, ни метаданные.

Первый удаляет метаданные реплики `'replica_name'` таблицы `database.table`.
Второй делает то же самое для всех реплицируемых таблиц в базе данных.
Третий делает то же самое для всех реплицируемых таблиц на локальном сервере.
Четвёртый полезен для удаления метаданных вышедшей из строя реплики, когда все остальные реплики таблицы были удалены. Для него требуется явно указать путь таблицы. Это должен быть тот же путь, который был передан в первый аргумент движка `ReplicatedMergeTree` при создании таблицы.

<div id="drop-database-replica">
  ## SYSTEM DROP DATABASE REPLICA
</div>

Неактивные реплики баз данных `Replicated` можно удалить, используя следующий синтаксис:

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

Аналогично `SYSTEM DROP REPLICA`, но удаляет путь к реплике базы данных `Replicated` из ZooKeeper, когда базы данных для выполнения `DROP DATABASE` уже нет. Обратите внимание: эта команда не удаляет реплики `ReplicatedMergeTree` (поэтому вам также может понадобиться `SYSTEM DROP REPLICA`). Имена сегмента и реплики — это имена, указанные в аргументах движка `Replicated` при создании базы данных. Кроме того, эти имена можно получить из столбцов `database_shard_name` и `database_replica_name` в `system.clusters`. Если предложение `FROM SHARD` отсутствует, то `replica_name` должно быть полным именем реплики в формате `shard_name|replica_name`.

<div id="drop-uncompressed-cache">
  ## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE
</div>

Очищает кэш несжатых данных.
Кэш несжатых данных включается и отключается с помощью настройки уровня запроса, пользователя или профиля [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache).
Его размер можно задать с помощью настройки уровня сервера [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size).

<div id="drop-compiled-expression-cache">
  ## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE
</div>

Очищает кэш скомпилированных выражений.
Кэш скомпилированных выражений включается и отключается с помощью настройки на уровне запроса, пользователя или профиля [`compile_expressions`](../../operations/settings/settings.md#compile_expressions).

<div id="drop-query-condition-cache">
  ## SYSTEM CLEAR|DROP QUERY CONDITION CACHE
</div>

Очищает кэш условий запроса.

<div id="drop-query-cache">
  ## SYSTEM CLEAR|DROP QUERY CACHE
</div>

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
```

Очищает [кэш запросов](../../operations/query-cache.md).
Если указан тег, удаляются только записи кэша запросов с этим тегом.

<div id="system-drop-schema-format">
  ## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE
</div>

Очищает кэш схем, загруженных из [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path).

Поддерживаемые варианты:

* Protobuf: Удаляет из памяти импортированные определения сообщений Protobuf.
* Files: Удаляет кэшированные файлы схем, хранящиеся локально в [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path) и создаваемые, когда `format_schema_source` имеет значение `query`.
  Note: Если цель не указана, очищаются оба кэша.

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

<div id="flush-logs">
  ## SYSTEM FLUSH LOGS
</div>

Сбрасывает буферизованные сообщения лога в системные таблицы, например `system.query_log`. В основном полезно для отладки, поскольку у большинства системных таблиц интервал сброса по умолчанию составляет 7,5 секунды.
Это также создаст системные таблицы, даже если очередь сообщений пуста.

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

Если вы не хотите сбрасывать всё, можно сбросить один или несколько отдельных журналов, указав либо их имя, либо имя их целевой таблицы:

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

<div id="reload-config">
  ## SYSTEM RELOAD CONFIG
</div>

Перезагружает конфигурацию ClickHouse. Используется, когда конфигурация хранится в ZooKeeper. Обратите внимание, что `SYSTEM RELOAD CONFIG` не перезагружает конфигурацию `USER`, хранящуюся в ZooKeeper: эта команда перезагружает только конфигурацию `USER`, которая хранится в `users.xml`. Чтобы перезагрузить всю конфигурацию `USER`, используйте `SYSTEM RELOAD USERS`

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

<div id="reload-users">
  ## SYSTEM RELOAD USERS
</div>

Перезагружает все хранилища данных доступа, включая users.xml, локальное дисковое хранилище данных доступа и реплицируемое (в ZooKeeper) хранилище данных доступа.

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

<div id="shutdown">
  ## SYSTEM SHUTDOWN
</div>

<CloudNotSupportedBadge />

Обычно останавливает ClickHouse (как `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

<div id="kill">
  ## SYSTEM KILL
</div>

Принудительно завершает процесс ClickHouse (например, `kill -9 {$ pid_clickhouse-server}`)

<div id="instrument">
  ## SYSTEM INSTRUMENT
</div>

Управляет точками инструментирования с помощью функции XRay в LLVM, доступной, если ClickHouse собран с `ENABLE_XRAY=1`.
Это позволяет выполнять отладку и профилирование в продакшне без изменения исходного кода и с минимальными накладными расходами.
Если точки инструментирования не добавлены, снижение производительности пренебрежимо мало, поскольку добавляется лишь дополнительный переход на ближайший
адрес в прологе и эпилоге тех функций, длина которых превышает 200 инструкций.

<div id="instrument-add">
  ### SYSTEM INSTRUMENT ADD
</div>

Добавляет новую точку инструментирования. Функции, для которых включена инструментация, можно просматривать в системной таблице [`system.instrumentation`](../../operations/system-tables/instrumentation.md). Для одной и той же функции можно добавить несколько обработчиков, и они будут выполняться в том же порядке, в котором была добавлена инструментация.
Функции, для которых нужно включить инструментацию, можно получить из системной таблицы [`system.symbols`](../../operations/system-tables/symbols.md).

Существует три типа обработчиков, которые можно добавлять к функциям:

**Синтаксис**

```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

где `FUNCTION` — любая функция или подстрока имени функции, например `QueryMetricLog::startQuery`, а обработчик — один из следующих

<div id="instrument-add-log">
  #### LOG
</div>

Выводит текст, переданный в качестве аргумента, и трассировку стека при `ENTRY` или `EXIT` функции.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

<div id="instrument-add-sleep">
  #### SLEEP
</div>

Приостанавливает выполнение на фиксированное число секунд либо при `ENTRY`, либо при `EXIT`:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

или для случайного количества секунд с равномерным распределением, указав min и max через пробел:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

<div id="instrument-add-profile">
  #### PROFILE
</div>

Измеряет время, затраченное между `ENTRY` и `EXIT` функции.
Результат профилирования сохраняется в [`system.trace_log`](../../operations/system-tables/trace_log.md) и может быть преобразован
в [формат событий Chrome Trace](../../operations/system-tables/trace_log.md#chrome-event-trace-format).

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

<div id="instrument-remove">
  ### SYSTEM INSTRUMENT REMOVE
</div>

Удаляет одну точку инструментирования с помощью:

```sql
SYSTEM INSTRUMENT REMOVE ID
```

все из них с помощью ключевого слова `ALL`:

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

набор идентификаторов из подзапроса:

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

или все точки инструментирования, соответствующие указанному function&#95;name:

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

Информацию о точках инструментирования можно получить из системной таблицы [`system.instrumentation`](../../operations/system-tables/instrumentation.md).

<div id="managing-distributed-tables">
  ## Управление распределёнными таблицами
</div>

ClickHouse может управлять [distributed таблицами](../../engines/table-engines/special/distributed.md). Когда пользователь вставляет данные в эти таблицы, ClickHouse сначала создаёт очередь данных, которые нужно отправить на узлы кластера, а затем отправляет их асинхронно. Управлять обработкой очереди можно с помощью запросов [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends), [FLUSH DISTRIBUTED](#flush-distributed) и [`START DISTRIBUTED SENDS`](#start-distributed-sends). Вы также можете синхронно вставлять данные в distributed таблицы с помощью настройки [`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert).

<div id="stop-distributed-sends">
  ### SYSTEM STOP DISTRIBUTED SENDS
</div>

Отключает фоновую отправку данных при вставке в distributed таблицы.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
Если параметр [`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica) включен (это значение по умолчанию), данные всё равно будут вставлены в локальный сегмент.
:::

<div id="flush-distributed">
  ### SYSTEM FLUSH DISTRIBUTED
</div>

Принудительно заставляет ClickHouse синхронно отправлять данные на узлы кластера. Если какие-либо узлы недоступны, ClickHouse генерирует исключение и останавливает выполнение запроса. Вы можете повторять запрос, пока он не завершится успешно; это произойдет, когда все узлы снова будут в сети.

Вы также можете переопределить некоторые настройки с помощью секции `SETTINGS`; это может быть полезно, чтобы обойти временные ограничения, такие как `max_concurrent_queries_for_all_users` или `max_memory_usage`.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
Каждый ожидающий блок хранится на диске с настройками исходного запроса INSERT, поэтому иногда может понадобиться переопределить эти настройки.
:::

<div id="start-distributed-sends">
  ### SYSTEM START DISTRIBUTED SENDS
</div>

Включает фоновую отправку данных при вставке данных в distributed таблицы.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<div id="stop-listen">
  ### SYSTEM STOP LISTEN
</div>

Закрывает сокет и корректно завершает существующие соединения с сервером на указанном порту по указанному протоколу.

Однако, если соответствующие настройки протокола не были указаны в конфигурации clickhouse-server, эта команда не даст никакого эффекта.

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

* Если указан модификатор `CUSTOM 'protocol'`, будет остановлен пользовательский протокол с указанным именем, определённый в разделе протоколов конфигурации сервера.
* Если указан модификатор `QUERIES ALL [EXCEPT .. [,..]]`, будут остановлены все протоколы, кроме указанных в предложении `EXCEPT`.
* Если указан модификатор `QUERIES DEFAULT [EXCEPT .. [,..]]`, будут остановлены все протоколы по умолчанию, кроме указанных в предложении `EXCEPT`.
* Если указан модификатор `QUERIES CUSTOM [EXCEPT .. [,..]]`, будут остановлены все пользовательские протоколы, кроме указанных в предложении `EXCEPT`.

<div id="start-listen">
  ### SYSTEM START LISTEN
</div>

Разрешает устанавливать новые соединения по указанным протоколам.

Однако если сервер на указанном порту и с указанным протоколом не был остановлен с помощью команды SYSTEM STOP LISTEN, эта команда не даст никакого эффекта.

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

<div id="managing-mergetree-tables">
  ## Управление таблицами семейства MergeTree
</div>

В ClickHouse можно управлять фоновыми процессами в таблицах [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

<div id="stop-merges">
  ### SYSTEM STOP MERGES
</div>

<CloudNotSupportedBadge />

Позволяет остановить фоновые слияния для таблиц семейства MergeTree:

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
`DETACH / ATTACH` таблицы запустит для неё фоновые слияния, даже если ранее они были остановлены для всех таблиц семейства MergeTree.
:::

<div id="start-merges">
  ### SYSTEM START MERGES
</div>

<CloudNotSupportedBadge />

Позволяет запускать фоновые слияния для таблиц семейства MergeTree:

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<div id="stop-ttl-merges">
  ### SYSTEM STOP TTL MERGES
</div>

<CloudNotSupportedBadge />

Позволяет остановить фоновое удаление старых данных в соответствии с [TTL-выражением](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) для таблиц семейства MergeTree:
Возвращает `Ok.`, даже если таблица не существует или не использует движок MergeTree. Возвращает ошибку, если база данных не существует:

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-ttl-merges">
  ### SYSTEM START TTL MERGES
</div>

<CloudNotSupportedBadge />

Позволяет запустить фоновое удаление устаревших данных в соответствии с [TTL-выражением](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) для таблиц семейства MergeTree:
Возвращает `Ok.`, даже если таблица не существует. Возвращает ошибку, если база данных не существует:

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="stop-moves">
  ### SYSTEM STOP MOVES
</div>

Позволяет остановить фоновое перемещение данных в соответствии с [TTL-выражением таблицы с условием TO VOLUME или TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) для таблиц семейства MergeTree:
Возвращает `Ok.` даже если таблица не существует. Возвращает ошибку, если база данных не существует:

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-moves">
  ### SYSTEM START MOVES
</div>

Позволяет запускать фоновые перемещения данных в соответствии с [TTL-выражением таблицы с предложениями TO VOLUME и TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) для таблиц семейства MergeTree:
Возвращает `Ok.`, даже если таблица не существует. Возвращает ошибку, если база данных не существует:

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="query_language-system-unfreeze">
  ### SYSTEM UNFREEZE
</div>

Удаляет замороженную резервную копию с указанным именем со всех дисков. Подробнее о разморозке отдельных частей см. в [ALTER TABLE table&#95;name UNFREEZE WITH NAME ](/ru/sql-reference/statements/alter/partition#unfreeze-partition)

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

<div id="wait-loading-parts">
  ### SYSTEM WAIT LOADING PARTS
</div>

Ожидает, пока не загрузятся все асинхронно загружаемые части данных таблицы (устаревшие части данных).

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

<div id="managing-replicatedmergetree-tables">
  ## Управление таблицами ReplicatedMergeTree
</div>

ClickHouse может управлять фоновыми процессами репликации в таблицах [ReplicatedMergeTree](/ru/engines/table-engines/mergetree-family/replication).

<div id="stop-fetches">
  ### SYSTEM STOP FETCHES
</div>

<CloudNotSupportedBadge />

Позволяет остановить фоновые загрузки вставленных частей для таблиц семейства `ReplicatedMergeTree`:
Всегда возвращает `Ok.` независимо от движка таблицы, даже если таблица или база данных не существует.

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-fetches">
  ### SYSTEM START FETCHES
</div>

<CloudNotSupportedBadge />

Позволяет запустить фоновые загрузки вставленных частей для таблиц семейства `ReplicatedMergeTree`:
Всегда возвращает `Ok.` независимо от движка таблицы, даже если таблица или база данных не существуют.

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replicated-sends">
  ### SYSTEM STOP REPLICATED SENDS
</div>

Позволяет остановить фоновую отправку новым частям, вставленным в таблицы семейства `ReplicatedMergeTree`, другим репликам в кластере:

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replicated-sends">
  ### SYSTEM START REPLICATED SENDS
</div>

Позволяет запустить фоновую отправку другим репликам в кластере новых частей, вставленных в таблицы семейства `ReplicatedMergeTree`:

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replication-queues">
  ### SYSTEM STOP REPLICATION QUEUES
</div>

Позволяет остановить фоновые задачи загрузки из очередей репликации, хранящихся в Zookeeper, для таблиц семейства `ReplicatedMergeTree`. Возможные типы фоновых задач — слияние, загрузка, мутация, DDL-операторы с предложением ON CLUSTER:

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replication-queues">
  ### SYSTEM START REPLICATION QUEUES
</div>

Позволяет запускать фоновые задачи загрузки из очередей репликации, хранящихся в ZooKeeper, для таблиц семейства `ReplicatedMergeTree`. Возможные типы фоновых задач — слияние, загрузка, мутации, DDL-операторы с предложением ON CLUSTER:

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-pulling-replication-log">
  ### SYSTEM STOP PULLING REPLICATION LOG
</div>

Прекращает загрузку новых записей из журнала репликации в очередь репликации таблицы `ReplicatedMergeTree`.

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-pulling-replication-log">
  ### SYSTEM START PULLING REPLICATION LOG
</div>

Отменяет действие команды `SYSTEM STOP PULLING REPLICATION LOG`.

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="sync-replica">
  ### SYSTEM SYNC REPLICA
</div>

Ожидает, пока таблица `ReplicatedMergeTree` синхронизируется с другими репликами в кластере, но не дольше `receive_timeout` секунд.

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

После выполнения этого оператора `[db.]replicated_merge_tree_family_table_name` забирает команды из общего журнала репликации в свою очередь репликации, после чего запрос ожидает, пока реплика не обработает все полученные команды. Поддерживаются следующие модификаторы:

* С `IF EXISTS` (доступно начиная с 25.6) запрос не выдаст ошибку, если таблица не существует. Это полезно при добавлении новой реплики в cluster, когда она уже включена в конфигурацию cluster, но таблица для неё ещё находится в процессе создания и синхронизации.
* Если указан модификатор `STRICT`, запрос ждёт, пока очередь репликации не опустеет. Вариант `STRICT` может так и не завершиться успешно, если в очереди репликации постоянно появляются новые записи.
* Если указан модификатор `LIGHTWEIGHT`, запрос ждёт только обработки записей `GET_PART`, `ATTACH_PART`, `DROP_RANGE`, `REPLACE_RANGE` и `DROP_PART`.
  Кроме того, модификатор `LIGHTWEIGHT` поддерживает необязательное предложение FROM &#39;srcReplicas&#39;, где &#39;srcReplicas&#39; — это список имён исходных реплик, разделённых запятыми. Это расширение позволяет выполнять более точечную синхронизацию, ограничиваясь только задачами репликации, поступающими от указанных исходных реплик.
* Если указан модификатор `PULL`, запрос получает новые записи очереди репликации из ZooKeeper, но не ждёт их обработки.

<div id="sync-database-replica">
  ### SYNC DATABASE REPLICA
</div>

Ожидает, пока указанная [база данных Replicated](/ru/engines/database-engines/replicated) не применит все изменения схемы из очереди DDL этой базы данных.

**Синтаксис**

```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

<div id="restart-replica">
  ### SYSTEM RESTART REPLICA
</div>

Позволяет повторно инициализировать состояние сеанса ZooKeeper для таблицы `ReplicatedMergeTree`, сравнить текущее состояние с ZooKeeper как с источником истины и при необходимости добавить задачи в очередь ZooKeeper.
Инициализация очереди репликации на основе данных ZooKeeper происходит так же, как и для оператора `ATTACH TABLE`. На короткое время таблица будет недоступна для каких-либо операций.

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

<div id="restore-replica">
  ### SYSTEM RESTORE REPLICA
</div>

Восстанавливает реплику, если данные [возможно] сохранились, но метаданные ZooKeeper утеряны.

Работает только для таблиц `ReplicatedMergeTree` в режиме только для чтения.

Запрос можно выполнить после:

* Потери корневого пути ZooKeeper `/`.
* Потери пути реплик `/replicas`.
* Потери пути отдельной реплики `/replicas/replica_name/`.

Реплика подключает найденные локально части и отправляет информацию о них в ZooKeeper.
Части, которые были на реплике до потери метаданных, не запрашиваются повторно с других реплик, если они не устарели (то есть восстановление реплики не означает повторную загрузку всех данных по сети).

:::note
Части во всех состояниях перемещаются в каталог `detached/`. Части, которые были активны до потери данных (committed), подключаются.
:::

<div id="restore-database-replica">
  ### SYSTEM RESTORE DATABASE REPLICA
</div>

Восстанавливает реплику, если данные [возможно] есть, но метаданные Zookeeper утрачены.

**Синтаксис**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**Пример**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**Синтаксис**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

Альтернативный синтаксис:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**Пример**

Создание таблицы на нескольких серверах. После утраты метаданных реплики в ZooKeeper таблица подключится в режиме только для чтения, так как метаданные отсутствуют. Последний запрос нужно выполнить на каждой реплике.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

Другой вариант:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

<div id="restart-replicas">
  ### SYSTEM RESTART REPLICAS
</div>

Позволяет повторно инициализировать состояние сеансов ZooKeeper для всех таблиц `ReplicatedMergeTree`, сравнивает текущее состояние с ZooKeeper как с источником истинного состояния и при необходимости добавляет задачи в очередь ZooKeeper

<div id="drop-filesystem-cache">
  ### SYSTEM CLEAR|DROP FILESYSTEM CACHE
</div>

Позволяет сбросить файловый кэш.

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

<div id="sync-file-cache">
  ### SYSTEM SYNC FILE CACHE
</div>

:::note
Эта команда слишком ресурсоёмка и потенциально может использоваться не по назначению.
:::

Выполняет системный вызов sync.

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

<div id="load-primary-key">
  ### SYSTEM LOAD PRIMARY KEY
</div>

Загружает первичные ключи для указанной таблицы или всех таблиц.

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

<div id="unload-primary-key">
  ### SYSTEM UNLOAD PRIMARY KEY
</div>

Выгружает первичные ключи указанной таблицы или всех таблиц.

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

<div id="managing-refreshable-materialized-views">
  ## Управление Refreshable Materialized Views
</div>

Команды для управления фоновыми задачами, которые выполняют [Refreshable Materialized Views](../../sql-reference/statements/create/view.md#refreshable-materialized-view)

При работе с ними следите за [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md).

<div id="stop-view-stop-views">
  ### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS
</div>

Останавливает периодическое обновление указанного представления или всех обновляемых представлений. Если обновление уже выполняется, оно также будет отменено.

Если представление находится в базе данных Replicated или Shared, `STOP VIEW` действует только на текущую реплику, а `STOP REPLICATED VIEW` — на все реплики.

:::note
Состояние остановки не сохраняется после перезапуска сервера. После перезапуска представления возобновят обновление в соответствии с настроенным для них расписанием.
В базах данных Replicated или Shared `SYSTEM STOP VIEW` действует только на текущую реплику. Используйте `SYSTEM STOP REPLICATED VIEW`, чтобы остановить обновление на всех репликах.
:::

```sql
SYSTEM STOP VIEW [db.]name
```

```sql
SYSTEM STOP VIEWS
```

<div id="start-view-start-views">
  ### SYSTEM START [REPLICATED] VIEW, START VIEWS
</div>

Включает периодическое обновление для указанного представления или для всех обновляемых представлений. Немедленное обновление при этом не запускается.

Если представление находится в базе данных Replicated или Shared, `START VIEW` отменяет эффект `STOP VIEW`, а `START REPLICATED VIEW` — эффект `STOP REPLICATED VIEW`. `START VIEW` также отменяет эффект `PAUSE VIEW`.

```sql
SYSTEM START VIEW [db.]name
```

```sql
SYSTEM START VIEWS
```

<div id="pause-view-pause-views">
  ### SYSTEM PAUSE VIEW, PAUSE VIEWS
</div>

Отключает периодическое обновление указанного представления или всех обновляемых представлений.
В отличие от `SYSTEM STOP VIEW`, `SYSTEM PAUSE VIEW` не прерывает уже выполняющееся обновление: текущему обновлению будет позволено завершиться, а предотвращены будут только последующие обновления.

Отменяется с помощью `SYSTEM START VIEW` или `SYSTEM START VIEWS`.

:::note
Состояние паузы не сохраняется после перезапуска server. После перезапуска представления возобновят обновление по настроенному расписанию.
В базах данных Replicated или Shared команда `SYSTEM PAUSE VIEW` влияет только на текущую реплику.
:::

```sql
SYSTEM PAUSE VIEW [db.]name
```

```sql
SYSTEM PAUSE VIEWS
```

<div id="refresh-view">
  ### SYSTEM REFRESH VIEW
</div>

Немедленно запускает внеплановое обновление указанного представления.

```sql
SYSTEM REFRESH VIEW [db.]name
```

<div id="wait-view">
  ### SYSTEM WAIT VIEW
</div>

Ожидает завершения текущего обновления. Если обновление не выполняется, возвращается немедленно. Если последняя попытка обновления завершилась неудачей, сообщает об ошибке.

Можно использовать сразу после создания нового refreshable materialized view (без ключевого слова EMPTY), чтобы дождаться завершения первоначального обновления.

Если представление находится в базе данных Replicated или Shared и обновление выполняется на другой реплике, ожидает завершения этого обновления.

```sql
SYSTEM WAIT VIEW [db.]name
```

<div id="cancel-view">
  ### SYSTEM CANCEL VIEW
</div>

Если на текущей реплике для указанного представления выполняется обновление, оно будет прервано и отменено. В противном случае команда ничего не делает.

```sql
SYSTEM CANCEL VIEW [db.]name
```

<div id="flush-object-storage-queue">
  ## SYSTEM FLUSH OBJECT STORAGE QUEUE
</div>

Блокирует выполнение, пока указанный файл не будет обработан указанной таблицей [S3Queue](../../engines/table-engines/integrations/s3queue.md) или [AzureQueue](../../engines/table-engines/integrations/azure-queue.md) либо не завершится для неё необратимой ошибкой. Если файл уже был обработан, команда возвращает управление немедленно. Вызывает ошибку, если обработка файла необратимо завершилась ошибкой (все повторные попытки исчерпаны).

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```