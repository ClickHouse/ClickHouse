---
toc_priority: 59
toc_title: clickhouse-copier
---

# clickhouse-copier {#clickhouse-copier}

Копирует данные из таблиц одного кластера в таблицы другого (или этого же) кластера.

Можно запустить несколько `clickhouse-copier` для разных серверах для выполнения одного и того же задания. Для синхронизации между процессами используется ZooKeeper.

После запуска, `clickhouse-copier`:

-   Соединяется с ZooKeeper и получает:

    -   Задания на копирование.
    -   Состояние заданий на копирование.

-   Выполняет задания.

        Каждый запущенный процесс выбирает "ближайший" шард исходного кластера и копирует данные в кластер назначения, при необходимости перешардируя их.

`clickhouse-copier` отслеживает изменения в ZooKeeper и применяет их «на лету».

Для снижения сетевого трафика рекомендуем запускать `clickhouse-copier` на том же сервере, где находятся исходные данные.

## Запуск Clickhouse-copier {#zapusk-clickhouse-copier}

Утилиту следует запускать вручную следующим образом:

``` bash
$ clickhouse-copier --daemon --config zookeeper.xml --task-path /task/path --base-dir /path/to/dir
```

Параметры запуска:

-   `daemon` - запускает `clickhouse-copier` в режиме демона.
-   `config` - путь к файлу `zookeeper.xml` с параметрами соединения с ZooKeeper.
-   `task-path` - путь к ноде ZooKeeper. Нода используется для синхронизации между процессами `clickhouse-copier` и для хранения заданий. Задания хранятся в `$task-path/description`.
-   `task-file` - необязательный путь к файлу с описанием конфигурация заданий для загрузки в ZooKeeper.
-   `task-upload-force` - Загрузить `task-file` в ZooKeeper даже если уже было загружено.
-   `base-dir` - путь к логам и вспомогательным файлам. При запуске `clickhouse-copier` создает в `$base-dir` подкаталоги `clickhouse-copier_YYYYMMHHSS_<PID>`. Если параметр не указан, то каталоги будут создаваться в каталоге, где `clickhouse-copier` был запущен.

## Формат Zookeeper.xml {#format-zookeeper-xml}

``` xml
<clickhouse>
    <logger>
        <level>trace</level>
        <size>100M</size>
        <count>3</count>
    </logger>

    <zookeeper>
        <node index="1">
            <host>127.0.0.1</host>
            <port>2181</port>
        </node>
    </zookeeper>
</clickhouse>
```

## Конфигурация заданий на копирование {#konfiguratsiia-zadanii-na-kopirovanie}

``` xml
<clickhouse>
    <!-- Configuration of clusters as in an ordinary server config -->
    <remote_servers>
        <source_cluster>
		    <!--
                source cluster & destination clusters accept exactly the same
                parameters as parameters for the usual Distributed table
                see https://clickhouse.com/docs/ru/engines/table-engines/special/distributed/
            -->
            <shard>
                <internal_replication>false</internal_replication>
                    <replica>
                        <host>127.0.0.1</host>
                        <port>9000</port>
						<!--
                        <user>default</user>
                        <password>default</password>
                        <secure>1</secure>
                        -->
                    </replica>
            </shard>
            ...
        </source_cluster>

        <destination_cluster>
        ...
        </destination_cluster>
    </remote_servers>

    <!-- How many simultaneously active workers are possible. If you run more workers superfluous workers will sleep. -->
    <max_workers>2</max_workers>

    <!-- Setting used to fetch (pull) data from source cluster tables -->
    <settings_pull>
        <readonly>1</readonly>
    </settings_pull>

    <!-- Setting used to insert (push) data to destination cluster tables -->
    <settings_push>
        <readonly>0</readonly>
    </settings_push>

    <!-- Common setting for fetch (pull) and insert (push) operations. Also, copier process context uses it.
         They are overlaid by <settings_pull/> and <settings_push/> respectively. -->
    <settings>
        <connect_timeout>3</connect_timeout>
        <!-- Sync insert is set forcibly, leave it here just in case. -->
        <insert_distributed_sync>1</insert_distributed_sync>
    </settings>

    <!-- Copying tasks description.
         You could specify several table task in the same task description (in the same ZooKeeper node), they will be performed
         sequentially.
    -->
    <tables>
        <!-- A table task, copies one table. -->
        <table_hits>
            <!-- Source cluster name (from <remote_servers/> section) and tables in it that should be copied -->
            <cluster_pull>source_cluster</cluster_pull>
            <database_pull>test</database_pull>
            <table_pull>hits</table_pull>

            <!-- Destination cluster name and tables in which the data should be inserted -->
            <cluster_push>destination_cluster</cluster_push>
            <database_push>test</database_push>
            <table_push>hits2</table_push>

            <!-- Engine of destination tables.
                 If destination tables have not be created, workers create them using columns definition from source tables and engine
                 definition from here.

                 NOTE: If the first worker starts insert data and detects that destination partition is not empty then the partition will
                 be dropped and refilled, take it into account if you already have some data in destination tables. You could directly
                 specify partitions that should be copied in <enabled_partitions/>, they should be in quoted format like partition column of
                 system.parts table.
            -->
            <engine>
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/hits2', '{replica}')
            PARTITION BY toMonday(date)
            ORDER BY (CounterID, EventDate)
            </engine>

            <!-- Sharding key used to insert data to destination cluster -->
            <sharding_key>jumpConsistentHash(intHash64(UserID), 2)</sharding_key>

            <!-- Optional expression that filter data while pull them from source servers -->
            <where_condition>CounterID != 0</where_condition>

            <!-- This section specifies partitions that should be copied, other partition will be ignored.
                 Partition names should have the same format as
                 partition column of system.parts table (i.e. a quoted text).
                 Since partition key of source and destination cluster could be different,
                 these partition names specify destination partitions.

                 NOTE: In spite of this section is optional (if it is not specified, all partitions will be copied),
                 it is strictly recommended to specify them explicitly.
                 If you already have some ready partitions on destination cluster they
                 will be removed at the start of the copying since they will be interpeted
                 as unfinished data from the previous copying!!!
            -->
            <enabled_partitions>
                <partition>'2018-02-26'</partition>
                <partition>'2018-03-05'</partition>
                ...
            </enabled_partitions>
        </table_hits>

        <!-- Next table to copy. It is not copied until previous table is copying. -->
        <table_visits>
        ...
        </table_visits>
        ...
    </tables>
</clickhouse>
```

`clickhouse-copier` отслеживает изменения `/task/path/description` и применяет их «на лету». Если вы поменяете, например, значение `max_workers`, то количество процессов, выполняющих задания, также изменится.

