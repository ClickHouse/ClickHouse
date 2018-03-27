# clickhouse-copier util

The util copies tables data from one cluster to new tables of other (possibly the same) cluster in distributed and fault-tolerant manner.

Configuration of copying tasks is set in special ZooKeeper node (called the `/description` node).
A ZooKeeper path to the description node is specified via `--task-path </task/path>` parameter.
So, node `/task/path/description` should contain special XML content describing copying tasks.

Simultaneously many `clickhouse-copier` processes located on any servers could execute the same task.
ZooKeeper node `/task/path/` is used by the processes to coordinate their work.
You must not add additional child nodes to `/task/path/`.

Currently you are responsible for manual launching of all `cluster-copier` processes.
You can launch as many processes as you want, whenever and wherever you want.
Each process try to select the nearest available shard of source cluster and copy some part of data (partition) from it to the whole
destination cluster (with resharding).
Therefore it makes sense to launch cluster-copier processes on the source cluster nodes to reduce the network usage.

Since the workers coordinate their work via ZooKeeper, in addition to `--task-path </task/path>` you have to specify ZooKeeper
cluster configuration via `--config-file <zookeeper.xml>` parameter. Example of `zookeeper.xml`:

```xml
   <yandex>
    <zookeeper>
        <node index="1">
            <host>127.0.0.1</host>
            <port>2181</port>
        </node>
    </zookeeper>
   </yandex>
```

When you run `clickhouse-copier --config-file <zookeeper.xml> --task-path </task/path>` the process connects to ZooKeeper cluster, reads tasks config from `/task/path/description` and executes them.

## Format of task config

Here is an example of `/task/path/description` content:

```xml
<yandex>
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
            <engine>ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/hits2', '{replica}')
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
                 If you already have some ready paritions on destination cluster they
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
        </table_visits>
        ...
        </table_visits>
        ...
    </tables>
</yandex>
```

cluster-copier processes watch for `/task/path/description` node update.
So, if you modify the config settings or `max_workers` params, they will be updated.

## Example

```bash
clickhouse-copier copier --daemon --config /path/to/copier/zookeeper.xml --task-path /clickhouse-copier/cluster1_tables_hits --base-dir /path/to/copier_logs
```

`--base-dir /path/to/copier_logs` specifies where auxilary and log files of the copier process will be saved.
In this case it will create `/path/to/copier_logs/clickhouse-copier_YYYYMMHHSS_<PID>/` dir with log and status-files.
If it is not specified it will use current dir (`/clickhouse-copier_YYYYMMHHSS_<PID>/` if it is run as a `--daemon`).
