---
slug: /ru/operations/clickhouse-keeper
sidebar_position: 66
sidebar_label: ClickHouse Keeper
---

# ClickHouse Keeper {#clickHouse-keeper}

Сервер ClickHouse использует сервис координации [ZooKeeper](https://zookeeper.apache.org/) для [репликации](../engines/table-engines/mergetree-family/replication.md) данных и выполнения [распределенных DDL запросов](../sql-reference/distributed-ddl.md). ClickHouse Keeper — это альтернативный сервис координации, совместимый с ZooKeeper.

## Детали реализации {#implementation-details}

ZooKeeper — один из первых широко известных сервисов координации с открытым исходным кодом. Он реализован на языке программирования Java, имеет достаточно простую и мощную модель данных. Алгоритм координации Zookeeper называется ZAB (ZooKeeper Atomic Broadcast). Он не гарантирует линеаризуемость операций чтения, поскольку каждый узел ZooKeeper обслуживает чтения локально. В отличие от ZooKeeper, ClickHouse Keeper реализован на C++ и использует алгоритм [RAFT](https://raft.github.io/), [реализация](https://github.com/eBay/NuRaft). Этот алгоритм позволяет достичь линеаризуемости чтения и записи, имеет несколько реализаций с открытым исходным кодом на разных языках.

По умолчанию ClickHouse Keeper предоставляет те же гарантии, что и ZooKeeper (линеаризуемость записей, нелинеаризуемость чтений). ClickHouse Keeper предоставляет совместимый клиент-серверный протокол, поэтому любой стандартный клиент ZooKeeper может использоваться для взаимодействия с ClickHouse Keeper. Снэпшоты и журналы имеют несовместимый с ZooKeeper формат, однако можно конвертировать данные Zookeeper в снэпшот ClickHouse Keeper с помощью `clickhouse-keeper-converter`. Межсерверный протокол ClickHouse Keeper также несовместим с ZooKeeper, поэтому создание смешанного кластера ZooKeeper / ClickHouse Keeper невозможно.

Система управления доступом (ACL) ClickHouse Keeper реализована так же, как в [ZooKeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#sc_ZooKeeperAccessControl). ClickHouse Keeper поддерживает тот же набор разрешений и идентичные схемы: `world`, `auth`, `digest`. Digest для аутентификации использует пару значений `username:password`. Пароль кодируется в Base64.

:::note
    Внешние интеграции не поддерживаются.
:::

## Конфигурация {#configuration}

ClickHouse Keeper может использоваться как равноценная замена ZooKeeper или как внутренняя часть сервера ClickHouse, но в обоих случаях конфигурация представлена файлом `.xml`. Главный тег конфигурации ClickHouse Keeper — это `<keeper_server>`. Параметры конфигурации:

-    `tcp_port` — порт для подключения клиента (по умолчанию для ZooKeeper: `2181`).
-    `tcp_port_secure` — зашифрованный порт для SSL-соединения между клиентом и сервером сервиса.
-    `server_id` — уникальный идентификатор сервера, каждый участник кластера должен иметь уникальный номер (1, 2, 3 и т.д.).
-    `log_storage_path` — путь к журналам координации, лучше хранить их на не нагруженном устройстве (актуально и для ZooKeeper).
-    `snapshot_storage_path` — путь к снэпшотам координации.

Другие общие параметры наследуются из конфигурации сервера ClickHouse (`listen_host`, `logger`, и т. д.).

Настройки внутренней координации находятся в `<keeper_server>.<coordination_settings>`:

-    `auto_forwarding` — разрешить пересылку запросов на запись от последователей лидеру (по умолчанию: true).
-    `dead_session_check_period_ms` — частота, с которой ClickHouse Keeper проверяет мертвые сессии и удаляет их, в миллисекундах (по умолчанию: 500).
-    `election_timeout_lower_bound_ms` — время, после которого последователь может инициировать перевыбор лидера, если не получил от него контрольный сигнал (по умолчанию: 1000).
-    `election_timeout_upper_bound_ms` — время, после которого последователь должен инициировать перевыбор лидера, если не получил от него контрольный сигнал (по умолчанию: 2000).
-    `leadership_expiry_ms` — Если лидер не получает ответа от достаточного количества последователей в течение этого промежутка времени, он добровольно отказывается от своего руководства. При настройке 0 автоматически устанавливается 20 - кратное значение `heart_beat_interval_ms`, а при настройке меньше 0 лидер не отказывается от лидерства (по умолчанию 0).
-    `force_sync` — вызывать `fsync` при каждой записи в журнал координации (по умолчанию: true).
-    `four_letter_word_white_list` — список разрешенных 4-х буквенных команд (по умолчанию: "conf,cons,crst,envi,ruok,srst,srvr,stat,wchc,wchs,dirs,mntr,isro").
-    `fresh_log_gap` — минимальное отставание от лидера в количестве записей журнала после которого последователь считает себя актуальным (по умолчанию: 200).
-    `heart_beat_interval_ms` — частота, с которой узел-лидер ClickHouse Keeper отправляет контрольные сигналы узлам-последователям, в миллисекундах (по умолчанию: 500).
-    `max_requests_batch_size` — количество запросов на запись, которые будут сгруппированы в один перед отправкой через RAFT (по умолчанию: 100).
-    `min_session_timeout_ms` — Min timeout for client session (ms) (default: 10000).
-    `operation_timeout_ms` — максимальное время ожидания для одной клиентской операции в миллисекундах (по умолчанию: 10000).
-    `quorum_reads` — выполнять запросы чтения аналогично запросам записи через консенсус RAFT (по умолчанию: false).
-    `raft_logs_level` — уровень логгирования сообщений в текстовый лог  (trace, debug и т. д.) (по умолчанию: default).
-    `reserved_log_items` — минимальное количество записей в журнале координации которые нужно сохранять после снятия снепшота (по умолчанию: 100000).
-    `rotate_log_storage_interval` — количество записей в журнале  координации для хранения в одном файле (по умолчанию: 100000).
-    `session_timeout_ms` — максимальное время ожидания для клиентской сессии в миллисекундах (по умолчанию: 30000).
-    `shutdown_timeout` — время ожидания завершения внутренних подключений при выключении, в миллисекундах (по умолчанию: 5000).
-    `snapshot_distance` — частота, с которой ClickHouse Keeper делает новые снэпшоты (по количеству записей в журналах) (по умолчанию: 100000).
-    `snapshots_to_keep` — количество снэпшотов для хранения (по умолчанию: 3).
-    `stale_log_gap` — время, после которого лидер считает последователя отставшим и отправляет ему снэпшот вместо журналов (по умолчанию: 10000).
-    `startup_timeout` — время отключения сервера, если он не подключается к другим участникам кворума, в миллисекундах (по умолчанию: 30000).


Конфигурация кворума находится в `<keeper_server>.<raft_configuration>` и содержит описание серверов. 

Единственный параметр для всего кворума — `secure`, который включает зашифрованное соединение для связи между участниками кворума. Параметру можно задать значение `true`, если для внутренней коммуникации между узлами требуется SSL-соединение, в ином случае не указывайте ничего.  

Параметры для каждого `<server>`:

-    `id` — идентификатор сервера в кворуме.
-    `hostname` — имя хоста, на котором размещен сервер.
-    `port` — порт, на котором серверу доступны соединения для внутренней коммуникации.


:::note
В случае изменения топологии кластера ClickHouse Keeper(например, замены сервера), удостоверьтесь, что вы сохраняеете отношение `server_id` - `hostname`, не переиспользуете существующие `server_id` для новых серверов и не перемешиваете идентификаторы. Подобные ошибки могут случаться, если вы используете автоматизацию при разворачивании кластера без логики сохранения идентификаторов.
:::

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

## Как запустить {#how-to-run}

ClickHouse Keeper входит в пакет `clickhouse-server`, просто добавьте конфигурацию `<keeper_server>` и запустите сервер ClickHouse как обычно. Если вы хотите запустить ClickHouse Keeper автономно, сделайте это аналогичным способом:

```bash
clickhouse-keeper --config /etc/your_path_to_config/config.xml --daemon
```

## 4-х буквенные команды {#four-letter-word-commands}

ClickHouse Keeper также поддерживает 4-х буквенные команды, почти такие же, как у Zookeeper. Каждая команда состоит из 4-х символов, например, `mntr`, `stat` и т. д. Несколько интересных команд: `stat` предоставляет общую информацию о сервере и подключенных клиентах, а `srvr` и `cons` предоставляют расширенные сведения о сервере и подключениях соответственно.

У 4-х буквенных команд есть параметр для настройки разрешенного списка `four_letter_word_allow_list`, который имеет значение по умолчанию "conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro".

Вы можете отправлять команды в ClickHouse Keeper через telnet или nc на порт для клиента.

```
echo mntr | nc localhost 9181
```

Ниже приведен подробный список 4-х буквенных команд:

- `ruok`: Проверяет, что сервер запущен без ошибок. В этом случае сервер ответит `imok`. В противном случае он не ответит. Ответ `imok` не обязательно означает, что сервер присоединился к кворуму, а указывает, что процесс сервера активен и привязан к указанному клиентскому порту. Используйте команду `stat` для получения подробной информации о состоянии кворума и клиентском подключении.

```
imok
```

- `mntr`: Выводит список переменных, которые используются для мониторинга работоспособности кластера.

```
zk_version      v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
zk_avg_latency  0
zk_max_latency  0
zk_min_latency  0
zk_packets_received     68
zk_packets_sent 68
zk_num_alive_connections        1
zk_outstanding_requests 0
zk_server_state leader
zk_znode_count  4
zk_watch_count  1
zk_ephemerals_count     0
zk_approximate_data_size        723
zk_open_file_descriptor_count   310
zk_max_file_descriptor_count    10240
zk_followers    0
zk_synced_followers     0
```

- `srvr`: Выводит информацию о сервере: его версию, роль участника кворума и т.п.

```
ClickHouse Keeper version: v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
Latency min/avg/max: 0/0/0
Received: 2
Sent : 2
Connections: 1
Outstanding: 0
Zxid: 34
Mode: leader
Node count: 4
```

- `stat`: Выводит краткие сведения о сервере и подключенных клиентах.

```
ClickHouse Keeper version: v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
Clients:
 192.168.1.1:52852(recved=0,sent=0)
 192.168.1.1:52042(recved=24,sent=48)
Latency min/avg/max: 0/0/0
Received: 4
Sent : 4
Connections: 1
Outstanding: 0
Zxid: 36
Mode: leader
Node count: 4
```

- `srst`: Сбрасывает статистику сервера. Команда влияет на результат вывода `srvr`, `mntr` и `stat`.

```
Server stats reset.
```

- `conf`: Выводит подробную информацию о серверной конфигурации.

```
server_id=1
tcp_port=2181
four_letter_word_allow_list=*
log_storage_path=./coordination/logs
snapshot_storage_path=./coordination/snapshots
max_requests_batch_size=100
session_timeout_ms=30000
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=1000
election_timeout_upper_bound_ms=2000
leadership_expiry_ms=0
reserved_log_items=1000000000000000
snapshot_distance=10000
auto_forwarding=true
shutdown_timeout=5000
startup_timeout=240000
raft_logs_level=information
snapshots_to_keep=3
rotate_log_storage_interval=100000
stale_log_gap=10000
fresh_log_gap=200
max_requests_batch_size=100
quorum_reads=false
force_sync=false
compress_logs=true
compress_snapshots_with_zstd_format=true
configuration_change_tries_count=20
```

- `cons`: Выводит полную информацию о подключениях/сессиях для всех клиентов, подключенных к этому серверу. Включает информацию о количестве принятых/отправленных пакетов, идентификаторе сессии, задержках операций, последней выполненной операции и т. д.

```
 192.168.1.1:52163(recved=0,sent=0,sid=0xffffffffffffffff,lop=NA,est=1636454787393,to=30000,lzxid=0xffffffffffffffff,lresp=0,llat=0,minlat=0,avglat=0,maxlat=0)
 192.168.1.1:52042(recved=9,sent=18,sid=0x0000000000000001,lop=List,est=1636454739887,to=30000,lcxid=0x0000000000000005,lzxid=0x0000000000000005,lresp=1636454739892,llat=0,minlat=0,avglat=0,maxlat=0)
```

- `crst`: Сбрасывает статистику подключений/сессий для всех подключений.

```
Connection stats reset.
```

- `envi`: Выводит подробную информацию о серверном окружении.

```
Environment:
clickhouse.keeper.version=v21.11.1.1-prestable-7a4a0b0edef0ad6e0aa662cd3b90c3f4acf796e7
host.name=ZBMAC-C02D4054M.local
os.name=Darwin
os.arch=x86_64
os.version=19.6.0
cpu.count=12
user.name=root
user.home=/Users/JackyWoo/
user.dir=/Users/JackyWoo/project/jd/clickhouse/cmake-build-debug/programs/
user.tmp=/var/folders/b4/smbq5mfj7578f2jzwn602tt40000gn/T/
```


- `dirs`: Показывает общий размер файлов снэпшотов и журналов в байтах.

```
snapshot_dir_size: 0
log_dir_size: 3875
```

- `isro`: Проверяет, что сервер работает в режиме только для чтения. Сервер ответит `ro`, если он находится в режиме только для чтения, или `rw`, если нет.

```
rw
```

- `wchs`: Показывает краткую информацию о количестве отслеживаемых путей (watches) на сервере.

```
1 connections watching 1 paths
Total watches:1
```

- `wchc`: Показывает подробную информацию об отслеживаемых путях (watches) на сервере в разбивке по сессиям. При этом выводится список сессий (подключений) с соответствующими отслеживаемыми путями. Обратите внимание, что в зависимости от количества отслеживаемых путей эта операция может быть дорогостоящей (т. е. повлиять на производительность сервера), используйте ее осторожно.

```
0x0000000000000001
    /clickhouse/task_queue/ddl
```

- `wchp`: Показывает подробную информацию об отслеживаемых путях (watches) на сервере в разбивке по пути. При этом выводится список путей (узлов) с соответствующими сессиями. Обратите внимание, что в зависимости от количества отселживаемых путей (watches) эта операция может быть дорогостоящей (т. е. повлиять на производительность сервера), используйте ее осторожно.

```
/clickhouse/task_queue/ddl
    0x0000000000000001
```

- `dump`: Выводит список незавершенных сеансов и эфемерных узлов. Команда работает только на лидере.

```
Sessions dump (2):
0x0000000000000001
0x0000000000000002
Sessions with Ephemerals (1):
0x0000000000000001
 /clickhouse/task_queue/ddl
```


## [экспериментально] Переход с ZooKeeper {#migration-from-zookeeper}

Плавный переход с ZooKeeper на ClickHouse Keeper невозможен, необходимо остановить кластер ZooKeeper, преобразовать данные и запустить ClickHouse Keeper. Утилита `clickhouse-keeper-converter` конвертирует журналы и снэпшоты ZooKeeper в снэпшот ClickHouse Keeper. Работа утилиты проверена только для версий ZooKeeper выше 3.4. Для миграции необходимо выполнить следующие шаги:

1. Остановите все узлы ZooKeeper.

2. Необязательно, но рекомендуется: найдите узел-лидер ZooKeeper, запустите и снова остановите его. Это заставит ZooKeeper создать консистентный снэпшот.

3. Запустите `clickhouse-keeper-converter` на лидере, например:

```bash
clickhouse-keeper-converter --zookeeper-logs-dir /var/lib/zookeeper/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/version-2 --output-dir /path/to/clickhouse/keeper/snapshots
```

4. Скопируйте снэпшот на узлы сервера ClickHouse с настроенным `keeper` или запустите ClickHouse Keeper вместо ZooKeeper. Снэпшот должен сохраняться на всех узлах: в противном случае пустые узлы могут захватить лидерство и сконвертированные данные могут быть отброшены на старте.

## Восстановление после потери кворума

Так как ClickHouse Keeper основан на протоколе Raft, он может оставаться работоспособным при отказе определенного количества нод в зависимости от размера кластера.
Например, для кластера из 3 нод, алгоритм кворума продолжает работать при отказе не более чем одной ноды.

Конфигурация кластера может быть изменена динамически с некоторыми ограничениями.
Переконфигурация также использует Raft, поэтому для добавления новой ноды кластера или исключения старой ноды требуется достижение кворума в рамках текущей конфигурации кластера.
Если в вашем кластере произошел отказ большего числа нод, чем допускает Raft для вашей текущей конфигурации и у вас нет возможности восстановить их работоспособность, Raft перестанет работать и не позволит изменить конфигурацию стандартным механизмом.

Тем не менее ClickHouse Keeper имеет возможность запуститься в режиме восстановления, который позволяет переконфигурировать кластер используя только одну ноду кластера.
Этот механизм может использоваться только как крайняя мера, когда вы не можете восстановить существующие ноды кластера или запустить новый сервер с тем же идентификатором.

Важно:
- Удостоверьтесь, что отказавшие ноды не смогут в дальнейшем подключиться к кластеру в будущем.
- Не запускайте новые ноды, пока не завершите процедуру ниже.

После того, как выполнили действия выше выполните следующие шаги.
1. Выберете одну ноду Keeper, которая станет новым лидером. Учтите, что данные с этой ноды будут использованы всем кластером, поэтому рекомендуется выбрать ноду с наиболее актуальным состоянием.
2. Перед дальнейшими действиями сделайте резервную копию данных из директорий `log_storage_path` и `snapshot_storage_path`.
3. Измените настройки на всех нодах кластера, которые вы собираетесь использовать.
4. Отправьте команду `rcvr` на ноду, которую вы выбрали, или остановите ее и запустите заново с аргументом `--force-recovery`. Это переведет ноду в режим восстановления.
5. Запускайте остальные ноды кластера по одной и проверяйте, что команда `mntr` возвращает `follower` в выводе состояния `zk_server_state` перед тем, как запустить следующую ноду.
6. Пока нода работает в режиме восстановления, лидер будет возвращать ошибку на запрос `mntr` пока кворум не будет достигнут с помощью новых нод. Любые запросы от клиентов и последователей будут возвращать ошибку.
7. После достижения кворума лидер перейдет в нормальный режим работы и станет обрабатывать все запросы через Raft. Удостоверьтесь, что запрос `mntr` возвращает `leader` в выводе состояния `zk_server_state`.
