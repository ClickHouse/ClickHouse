---
toc_priority: 66
toc_title: ClickHouse Keeper
---

# [пре-продакшн] ClickHouse Keeper

Сервер ClickHouse использует сервис координации [ZooKeeper](https://zookeeper.apache.org/) для [репликации](../engines/table-engines/mergetree-family/replication.md) данных и выполнения [распределенных DDL запросов](../sql-reference/distributed-ddl.md). ClickHouse Keeper — это альтернативный сервис координации, совместимый с ZooKeeper.

!!! warning "Предупреждение"
    ClickHouse Keeper находится в стадии пре-продакшн и тестируется в CI ClickHouse и на нескольких внутренних инсталляциях.

## Детали реализации

ZooKeeper — один из первых широко известных сервисов координации с открытым исходным кодом. Он реализован на языке программирования Java, имеет достаточно простую и мощную модель данных. Алгоритм координации Zookeeper называется ZAB (ZooKeeper Atomic Broadcast). Он не гарантирует линеаризуемость операций чтения, поскольку каждый узел ZooKeeper обслуживает чтения локально. В отличие от ZooKeeper, ClickHouse Keeper реализован на C++ и использует алгоритм [RAFT](https://raft.github.io/), [реализация](https://github.com/eBay/NuRaft). Этот алгоритм позволяет достичь линеаризуемости чтения и записи, имеет несколько реализаций с открытым исходным кодом на разных языках.

По умолчанию ClickHouse Keeper предоставляет те же гарантии, что и ZooKeeper (линеаризуемость записей, последовательная согласованность чтений). У него есть совместимый клиент-серверный протокол, поэтому любой стандартный клиент ZooKeeper может использоваться для взаимодействия с ClickHouse Keeper. Снэпшоты и журналы имеют несовместимый с ZooKeeper формат, однако можно конвертировать данные Zookeeper в снэпшот ClickHouse Keeper с помощью `clickhouse-keeper-converter`. Межсерверный протокол ClickHouse Keeper также несовместим с ZooKeeper, поэтому создание смешанного кластера ZooKeeper / ClickHouse Keeper невозможно.

## Конфигурация

ClickHouse Keeper может использоваться как равноценная замена ZooKeeper или как внутренняя часть сервера ClickHouse, но в обоих случаях конфигурация представлена файлом `.xml`. Главный тег конфигурации ClickHouse Keeper — это `<keeper_server>`. Параметры конфигурации:

-    `tcp_port` — порт для подключения клиента (по умолчанию для ZooKeeper: `2181`).
-    `tcp_port_secure` — зашифрованный порт для подключения клиента.
-    `server_id` — уникальный идентификатор сервера, каждый участник кластера должен иметь уникальный номер&nbsp;(1,&nbsp;2,&nbsp;3&nbsp;и&nbsp;т.&nbsp;д.).
-    `log_storage_path` — путь к журналам координации, лучше хранить их на незанятом устройстве (актуально и для ZooKeeper).
-    `snapshot_storage_path` — путь к снэпшотам координации.

Другие общие параметры наследуются из конфигурации сервера ClickHouse (`listen_host`, `logger`, и т. д.).

Настройки внутренней координации находятся в `<keeper_server>.<coordination_settings>`:

-    `operation_timeout_ms` — максимальное время ожидания для одной клиентской операции в миллисекундах (по умолчанию: 10000).
-    `session_timeout_ms` — максимальное время ожидания для клиентской сессии в миллисекундах (по умолчанию: 30000).
-    `dead_session_check_period_ms` — частота, с которой ClickHouse Keeper проверяет мертвые сессии и удаляет их, в миллисекундах (по умолчанию: 500).
-    `heart_beat_interval_ms` — частота, с которой узел-лидер ClickHouse Keeper отправляет хартбиты узлам-последователям, в миллисекундах (по умолчанию: 500).
-    `election_timeout_lower_bound_ms` — время, после которого последователь может инициировать выборы лидера, если не получил от него сердцебиения (по умолчанию: 1000).
-    `election_timeout_upper_bound_ms` — время, после которого последователь должен инициировать выборы лидера, если не получил от него сердцебиения (по умолчанию: 2000).
-    `rotate_log_storage_interval` — количество записей в журнале  координации для хранения в одном файле (по умолчанию: 100000).
-    `reserved_log_items` — минимальное количество записей в журнале координации которые нужно сохранять после снятия снепшота (по умолчанию: 100000).
-    `snapshot_distance` — частота, с которой ClickHouse Keeper делает новые снэпшоты (по количеству записей в журналах), в миллисекундах (по умолчанию: 100000).
-    `snapshots_to_keep` — количество снэпшотов для сохранения (по умолчанию: 3).
-    `stale_log_gap` — время, после которого лидер считает последователя устаревшим и отправляет ему снэпшот вместо журналов (по умолчанию: 10000).
-    `fresh_log_gap` — максимальное отставание от лидера в количестве записей журнала после которого последователь считает себя не отстающим (по умолчанию: 200).
-    `max_requests_batch_size` — количество запросов на запись, которые будут сгруппированы в один перед отправкой через RAFT (по умолчанию: 100).
-    `force_sync` — вызывать `fsync` при каждой записи в журнал координации (по умолчанию: true).
-    `quorum_reads` — выполнять запросы чтения аналогично запросам записи через весь консенсус RAFT с негативным эффектом на производительность и размер журналов (по умолчанию: false).
-    `raft_logs_level` — уровень логгирования сообщений в текстовый лог  (trace, debug и т. д.) (по умолчанию: information).
-    `auto_forwarding` — разрешить пересылку запросов на запись от последователей лидеру (по умолчанию: true).
-    `shutdown_timeout` — время ожидания завершения внутренних подключений и выключения, в миллисекундах (по умолчанию: 5000).
-    `startup_timeout` — время отключения сервера, если он не подключается к другим участникам кворума, в миллисекундах (по умолчанию: 30000).

Конфигурация кворума находится в `<keeper_server>.<raft_configuration>` и содержит описание серверов. Единственный параметр для всего кворума — `secure`, который включает зашифрованное соединение для связи между участниками кворума. Параметры для каждого `<server>`:

-    `id` — идентификатор сервера в кворуме.
-    `hostname` — имя хоста, на котором размещен сервер.
-    `port` — порт, на котором серверу доступны соединения для внутренней коммуникации.


Примеры конфигурации кворума с тремя узлами можно найти в [интеграционных тестах](https://github.com/ClickHouse/ClickHouse/tree/master/tests/integration) с префиксом `test_keeper_`. Пример конфигурации для сервера №1:

```xml
<keeper_server>
    <tcp_port>2181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

    <coordination_settings>
        <operation_timeout_ms>10000</operation_timeout_ms>
        <session_timeout_ms>30000</session_timeout_ms>
        <raft_logs_level>trace</raft_logs_level>
    </coordination_settings>

    <raft_configuration>
        <server>
            <id>1</id>
            <hostname>zoo1</hostname>
            <port>9444</port>
        </server>
        <server>
            <id>2</id>
            <hostname>zoo2</hostname>
            <port>9444</port>
        </server>
        <server>
            <id>3</id>
            <hostname>zoo3</hostname>
            <port>9444</port>
        </server>
    </raft_configuration>
</keeper_server>
```

## Как запустить

ClickHouse Keeper входит в пакет `clickhouse-server`, просто добавьте кофигурацию `<keeper_server>` и запустите сервер ClickHouse как обычно. Если вы хотите запустить ClickHouse Keeper автономно, сделайте это аналогичным способом:

```bash
clickhouse-keeper --config /etc/your_path_to_config/config.xml --daemon
```

## [экспериментально] Переход с ZooKeeper

Плавный переход с ZooKeeper на ClickHouse Keeper невозможен, необходимо остановить кластер ZooKeeper, преобразовать данные и запустить ClickHouse Keeper. Утилита `clickhouse-keeper-converter` конвертирует журналы и снэпшоты ZooKeeper в снэпшот ClickHouse Keeper. Работа утилиты проверена только для версий ZooKeeper выше 3.4. Для миграции необходимо выполнить следующие шаги:

1. Остановите все узлы ZooKeeper.

2. Необязательно, но рекомендуется: найдите узел-лидер ZooKeeper, запустите и снова остановите его. Это заставит ZooKeeper создать консистентный снэпшот.

3. Запустите `clickhouse-keeper-converter` на лидере, например:

```bash
clickhouse-keeper-converter --zookeeper-logs-dir /var/lib/zookeeper/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/version-2 --output-dir /path/to/clickhouse/keeper/snapshots
```

4. Скопируйте снэпшот на узлы сервера ClickHouse с настроенным `keeper` или запустите ClickHouse Keeper вместо ZooKeeper. Снэпшот должен сохраняться на всех узлах: в противном случае пустые узлы могут захватить лидерство и сконвертированные данные могут быть отброшены на старте.

[Original article](https://clickhouse.com/docs/en/operations/clickhouse-keeper/) <!--hide-->
