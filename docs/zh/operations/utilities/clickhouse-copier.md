# clickhouse-copier {#clickhouse-copier}

将数据从一个群集中的表复制到另一个（或相同）群集中的表。

您可以运行多个 `clickhouse-copier` 不同服务器上的实例执行相同的作业。 ZooKeeper用于同步进程。

开始后, `clickhouse-copier`:

-   连接到ZooKeeper并且接收:

    -   复制作业。
    -   复制作业的状态。

-   它执行的工作。

        每个正在运行的进程都会选择源集群的“最接近”分片，然后将数据复制到目标集群，并在必要时重新分片数据。

`clickhouse-copier` 跟踪ZooKeeper中的更改，并实时应用它们。

为了减少网络流量，我们建议运行 `clickhouse-copier` 在源数据所在的同一服务器上。

## 运行Clickhouse-copier {#running-clickhouse-copier}

该实用程序应手动运行:

``` bash
clickhouse-copier --daemon --config zookeeper.xml --task-path /task/path --base-dir /path/to/dir
```

参数:

-   `daemon` — 在守护进程模式下启动`clickhouse-copier`。
-   `config` — `zookeeper.xml`文件的路径，其中包含用于连接ZooKeeper的参数。
-   `task-path` — ZooKeeper节点的路径。 该节点用于同步`clickhouse-copier`进程和存储任务。 任务存储在`$task-path/description`中。
-   `task-file` — 可选的非必须参数, 指定一个包含任务配置的参数文件, 用于初始上传到ZooKeeper。
-   `task-upload-force` — 即使节点已经存在，也强制上载`task-file`。
-   `base-dir` — 日志和辅助文件的路径。 启动时，`clickhouse-copier`在`$base-dir`中创建`clickhouse-copier_YYYYMMHHSS_<PID>`子目录。 如果省略此参数，则会在启动`clickhouse-copier`的目录中创建目录。



## Zookeeper.xml格式 {#format-of-zookeeper-xml}

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

## 复制任务的配置 {#configuration-of-copying-tasks}

``` xml
<clickhouse>
    <!-- Configuration of clusters as in an ordinary server config -->
    <remote_servers>
        <source_cluster>
            <shard>
                <internal_replication>false</internal_replication>
                    <replica>
                        <host>127.0.0.1</host>
                        <port>9000</port>
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

`clickhouse-copier` 跟踪更改 `/task/path/description` 并在飞行中应用它们。 例如，如果你改变的值 `max_workers`，运行任务的进程数也会发生变化。

[原始文章](https://clickhouse.com/docs/en/operations/utils/clickhouse-copier/) <!--hide-->
