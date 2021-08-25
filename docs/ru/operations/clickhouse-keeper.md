---
toc_priority: 66
toc_title: ClickHouse Keeper
---

# [препродакшн] Clickhouse-keeper

Сервер ClickHouse использует сервис координации [ZooKeeper](https://zookeeper.apache.org/) для [репликации](../engines/table-engines/mergetree-family/replication.md) данных и выполнения [распределенных DDL запросов](../sql-reference/distributed-ddl.md). ClickHouse Keeper — это альтернативный сервис координации, совместимый с ZooKeeper.

!!! warning "Внимание"
    Эта функция сейчас в стадии препродакшн, тестируется в непрерывной интеграции (CI) на нескольких внутренних установках.

## Детали реализации

ZooKeeper — один из первых широко известных сервисов координации с открытым исходным кодом: реализован на Java, имеет достаточно простую и мощную модель данных. Алгоритм координации Zookeeper называется ZAB (ZooKeeper Atomic Broadcast). Он не гарантирует линеаризуемость операций чтения, поскольку каждый узел ZooKeeper обслуживает чтения локально. В отличие от ZooKeeper, `clickhouse-keeper` реализован на C++ и использует алгоритм [RAFT](https://raft.github.io/), [реализация](https://github.com/eBay/NuRaft). Этот алгоритм обеспечивает линеаризуемость чтения и записи, имеет несколько реализаций с открытым исходным кодом на разных языках.

По умолчанию `clickhouse-keeper` предоставляет те же гарантии, что и ZooKeeper (линеаризуемость записи, нелинеаризуемость чтения). У него есть совместимый протокол клиент-сервер, поэтому любой стандартный клиент ZooKeeper может использоваться для взаимодействия с `clickhouse-keeper`. Снэпшоты и журналы имеют несовместимый с ZooKeeper формат, однако можно конвертировать данные Zookeeper в снэпшот `clickhouse-keeper` с помощью `clickhouse-keeper-converter`. Межсерверный протокол `clickhouse-keeper` также несовместим с ZooKeeper, поэтому смешанный кластер ZooKeeper / clickhouse-keeper невозможен.

## Конфигурация

`clickhouse-keeper` может использоваться как равноценная замена ZooKeeper или как внутренняя часть сервера ClickHouse, но в обоих случаях конфигурация представлена файлом `.xml`. Главный тег конфигурации `clickhouse-keeper` — это `<keeper_server>`. Параметры конфигурации:

-    `tcp_port` — порт для подключения клиента (по умолчанию для ZooKeeper: `2181`)
-    `tcp_port_secure` — зашифрованный порт для подключения клиента
-    `server_id` — уникальный идентификатор сервера, каждый участник кластера должен иметь уникальный номер (1, 2, 3 и т. д.)
-    `log_storage_path` — путь к журналам координации, лучше хранить их на незанятом устройстве (актуально и для ZooKeeper)
-    `snapshot_storage_path` — путь к снэпшотам координации

Другие общие параметры наследуются из конфигурации `clickhouse server` (`listen_host`, `logger`, и т. д.)

Настройки внутренней координации находятся в `<keeper_server>.<coordination_settings>`:

-    `operation_timeout_ms` — интервал одной клиентской операции (по умолчанию: 10000)
-    `session_timeout_ms` — интервал одной клиентской сессии (по умолчанию: 30000)
-    `dead_session_check_period_ms` — как часто `clickhouse-keeper` проверяет мертвые сессии и удаляет их (по умолчанию: 500)
-    `heart_beat_interval_ms` — как часто узел-лидер `clickhouse-keeper` отправляет сердцебиение узлам-последователям (по умолчанию: 500)
-    `election_timeout_lower_bound_ms` — если последователь не получил сердцебиения от лидера в этом интервале, он может инициировать выборы лидера (по умолчанию: 1000)
-    `election_timeout_upper_bound_ms` — если последователь не получил сердцебиения от лидера в этом интервале, он должен инициировать выборы лидера (по умолчанию: 2000)
-    `rotate_log_storage_interval` — сколько записей журнала хранить в одном файле (по умолчанию: 100000)
-    `reserved_log_items` — сколько записей журнала координации хранить перед сжатием (по умолчанию: 100000)
-    `snapshot_distance` — как часто `clickhouse-keeper` делает новые снэпшоты (по количеству записей в журналах) (по умолчанию: 100000)
-    `snapshots_to_keep` — сколько снэпшотов хранить (по умолчанию: 3)
-    `stale_log_gap` — порог, после которого лидер считает последователя устаревшим и отправляет ему снэпшот вместо журналов (по умолчанию: 10000)
-    `fresh_log_gap` - когда узел обновляется (по умолчанию: 200)
-    `max_requests_batch_size` - максимальный размер пакета в серии запросов, прежде чем он будет отправлен в RAFT (по умолчанию: 100)
-    `force_sync` — вызывать `fsync` при каждой записи в журнал координации (по умолчанию: true)
-    `quorum_reads` - выполнять запросы чтения аналогично запросам записи через весь консенсус RAFT с одинаковой скоростью (по умолчанию: false)
-    `raft_logs_level` — уровень, на котором ведется текстовый журнал координации (трассировка, отладка и т. д.) (по умолчанию: значение системы по умолчанию)
-    `auto_forwarding` - разрешить пересылку запросов на запись от последователей лидеру (по умолчанию: true)
-    `shutdown_timeout` — ждать завершения внутренних подключений и выключения (мс) (по умолчанию: 5000)
-    `startup_timeout` — если сервер не подключается к другим участникам кворума в течение указанного интервала, он будет отключен (мс) (по умолчанию: 30000)

Конфигурация кворума находится в `<keeper_server>.<raft_configuration>` и содержит описание серверов. Единственный параметр для всего кворума — `secure`, который разрешает зашифрованное соединение для связи между участниками кворума. Параметры для каждого `<server>`:

-    `id` — идентификатор сервера в кворуме
-    `hostname` — имя хоста, на котором размещен сервер
-    `port` — порт, на котором серверу доступны соединения


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

`clickhouse-keeper` входит в пакет сервера ClickHouse, просто добавьте кофигурацию `<keeper_server>` и запустите сервер ClickHouse как обычно. Если вы хотите запустить `clickhouse-keeper` автономно, сделайте это аналогичным способом:

```bash
clickhouse-keeper --config /etc/your_path_to_config/config.xml --daemon
```

## [экспериментально] Переход с ZooKeeper

Плавный переход с ZooKeeper на `clickhouse-keeper` невозможен, необходимо остановить кластер ZooKeeper, преобразовать данные и запустить `clickhouse-keeper`. `clickhouse-keeper-converter` конвертирует журналы и снэпшоты ZooKeeper в снэпшот `clickhouse-keeper`. Это работает только для версии ZooKeeper выше 3.4. Шаги перехода:

1. Остановите все узлы ZooKeeper.

2. Необязательно, но рекомендуется: найдите лидер, запустите и снова остановите его. Это заставит ZooKeeper создать согласованный снэпшот.

3. Запустите `clickhouse-keeper-converter` на лидере, например:

```bash
clickhouse-keeper-converter --zookeeper-logs-dir /var/lib/zookeeper/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/version-2 --output-dir /path/to/clickhouse/keeper/snapshots
```

4. Скопируйте снэпшот на узлы сервера ClickHouse с настроенным `keeper` или запустите `clickhouse-keeper` вместо ZooKeeper. Снэпшот должен сохраняться на всех узлах: в противном случае пустые узлы будут быстрее, и один из них может стать лидером.

[Original article](https://clickhouse.tech/docs/en/operations/clickhouse-keeper/) <!--hide-->