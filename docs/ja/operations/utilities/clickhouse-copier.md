---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: "\u30AF\u30EA\u30C3\u30AF\u30CF\u30A6\u30B9-\u8907\u5199\u6A5F"
---

# クリックハウス-複写機 {#clickhouse-copier}

コピーデータからのテーブルを一つクラスターテーブルの他の同クラスター

複数実行できます `clickhouse-copier` インスタンスの異なるサーバーを行う仕事です。 ZooKeeperはプロセスの同期に使用されます。

開始後, `clickhouse-copier`:

-   ZooKeeperに接続し、受信します:

    -   ジョブのコピー。
    -   コピージョブの状態。

-   これは、ジョブを実行します。

    各実行中のプロセスは、 “closest” ザ-シャーのソースクラスタのデータ転送先のクラスター resharding場合はそのデータが必要です。

`clickhouse-copier` ZooKeeperの変更を追跡し、その場でそれらを適用します。

ネットワークトラフィ `clickhouse-copier` ソースデータが配置されている同じサーバー上。

## クリックハウスコピー機の実行 {#running-clickhouse-copier}

このユーテ:

``` bash
$ clickhouse-copier copier --daemon --config zookeeper.xml --task-path /task/path --base-dir /path/to/dir
```

パラメータ:

-   `daemon` — Starts `clickhouse-copier` デーモンモードで。
-   `config` — The path to the `zookeeper.xml` ZooKeeperへの接続のためのパラメータを持つファイル。
-   `task-path` — The path to the ZooKeeper node. This node is used for syncing `clickhouse-copier` プロセスとタスクの保存。 タスクは `$task-path/description`.
-   `task-file` — Optional path to file with task configuration for initial upload to ZooKeeper.
-   `task-upload-force` — Force upload `task-file` ノードが既に存在していても。
-   `base-dir` — The path to logs and auxiliary files. When it starts, `clickhouse-copier` 作成 `clickhouse-copier_YYYYMMHHSS_<PID>` サブディレクトリ `$base-dir`. このパラメーターを省略すると、ディレクトリは次の場所に作成されます `clickhouse-copier` 発売された。

## 飼育係の形式。xml {#format-of-zookeeper-xml}

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

## コピータスクの構成 {#configuration-of-copying-tasks}

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

`clickhouse-copier` の変更を追跡します `/task/path/description` そしてその場でそれらを適用します。 たとえば、次の値を変更すると `max_workers`、タスクを実行しているプロセスの数も変更されます。

[元の記事](https://clickhouse.com/docs/en/operations/utils/clickhouse-copier/) <!--hide-->
