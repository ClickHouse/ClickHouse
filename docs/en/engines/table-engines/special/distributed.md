---
toc_priority: 33
toc_title: Distributed
---

# Distributed Table Engine {#distributed}

Tables with Distributed engine do not store any data by their own, but allow distributed query processing on multiple servers.
Reading is automatically parallelized. During a read, the table indexes on remote servers are used, if there are any.

The Distributed engine accepts parameters:

-   the cluster name in the server’s config file

-   the name of a remote database

-   the name of a remote table

-   (optionally) sharding key

-   (optionally) policy name, it will be used to store temporary files for async send

    See also:

    -   [insert_distributed_sync](../../../operations/settings/settings.md#insert_distributed_sync) setting
    -   [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) for the examples

Example:

``` sql
Distributed(logs, default, hits[, sharding_key[, policy_name]])
```

Data will be read from all servers in the `logs` cluster, from the default.hits table located on every server in the cluster.
Data is not only read but is partially processed on the remote servers (to the extent that this is possible).
For example, for a query with GROUP BY, data will be aggregated on remote servers, and the intermediate states of aggregate functions will be sent to the requestor server. Then data will be further aggregated.

Instead of the database name, you can use a constant expression that returns a string. For example: currentDatabase().

logs – The cluster name in the server’s config file.

Clusters are set like this:

``` xml
<remote_servers>
    <logs>
        <shard>
            <!-- Optional. Shard weight when writing data. Default: 1. -->
            <weight>1</weight>
            <!-- Optional. Whether to write data to just one of the replicas. Default: false (write data to all replicas). -->
            <internal_replication>false</internal_replication>
            <replica>
                <!-- Optional. Priority of the replica for load balancing (see also load_balancing setting). Default: 1 (less value has more priority). -->
                <priority>1</priority>
                <host>example01-01-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-01-2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <weight>2</weight>
            <internal_replication>false</internal_replication>
            <replica>
                <host>example01-02-1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>example01-02-2</host>
                <secure>1</secure>
                <port>9440</port>
            </replica>
        </shard>
    </logs>
</remote_servers>
```

Here a cluster is defined with the name `logs` that consists of two shards, each of which contains two replicas.
Shards refer to the servers that contain different parts of the data (in order to read all the data, you must access all the shards).
Replicas are duplicating servers (in order to read all the data, you can access the data on any one of the replicas).

Cluster names must not contain dots.

The parameters `host`, `port`, and optionally `user`, `password`, `secure`, `compression` are specified for each server:
- `host` – The address of the remote server. You can use either the domain or the IPv4 or IPv6 address. If you specify the domain, the server makes a DNS request when it starts, and the result is stored as long as the server is running. If the DNS request fails, the server doesn’t start. If you change the DNS record, restart the server.
- `port` – The TCP port for messenger activity (`tcp_port` in the config, usually set to 9000). Do not confuse it with http\_port.
- `user` – Name of the user for connecting to a remote server. Default value: default. This user must have access to connect to the specified server. Access is configured in the users.xml file. For more information, see the section [Access rights](../../../operations/access-rights.md).
- `password` – The password for connecting to a remote server (not masked). Default value: empty string.
- `secure` - Use ssl for connection, usually you also should define `port` = 9440. Server should listen on `<tcp_port_secure>9440</tcp_port_secure>` and have correct certificates.
- `compression` - Use data compression. Default value: true.

When specifying replicas, one of the available replicas will be selected for each of the shards when reading. You can configure the algorithm for load balancing (the preference for which replica to access) – see the [load\_balancing](../../../operations/settings/settings.md#settings-load_balancing) setting.
If the connection with the server is not established, there will be an attempt to connect with a short timeout. If the connection failed, the next replica will be selected, and so on for all the replicas. If the connection attempt failed for all the replicas, the attempt will be repeated the same way, several times.
This works in favour of resiliency, but does not provide complete fault tolerance: a remote server might accept the connection, but might not work, or work poorly.

You can specify just one of the shards (in this case, query processing should be called remote, rather than distributed) or up to any number of shards. In each shard, you can specify from one to any number of replicas. You can specify a different number of replicas for each shard.

You can specify as many clusters as you wish in the configuration.

To view your clusters, use the `system.clusters` table.

The Distributed engine allows working with a cluster like a local server. However, the cluster is inextensible: you must write its configuration in the server config file (even better, for all the cluster’s servers).

The Distributed engine requires writing clusters to the config file. Clusters from the config file are updated on the fly, without restarting the server. If you need to send a query to an unknown set of shards and replicas each time, you don’t need to create a Distributed table – use the `remote` table function instead. See the section [Table functions](../../../sql-reference/table-functions/index.md).

There are two methods for writing data to a cluster:

First, you can define which servers to write which data to and perform the write directly on each shard. In other words, perform INSERT in the tables that the distributed table “looks at”. This is the most flexible solution as you can use any sharding scheme, which could be non-trivial due to the requirements of the subject area. This is also the most optimal solution since data can be written to different shards completely independently.

Second, you can perform INSERT in a Distributed table. In this case, the table will distribute the inserted data across the servers itself. In order to write to a Distributed table, it must have a sharding key set (the last parameter). In addition, if there is only one shard, the write operation works without specifying the sharding key, since it doesn’t mean anything in this case.

Each shard can have a weight defined in the config file. By default, the weight is equal to one. Data is distributed across shards in the amount proportional to the shard weight. For example, if there are two shards and the first has a weight of 9 while the second has a weight of 10, the first will be sent 9 / 19 parts of the rows, and the second will be sent 10 / 19.

Each shard can have the `internal_replication` parameter defined in the config file.

If this parameter is set to `true`, the write operation selects the first healthy replica and writes data to it. Use this alternative if the Distributed table “looks at” replicated tables. In other words, if the table where data will be written is going to replicate them itself.

If it is set to `false` (the default), data is written to all replicas. In essence, this means that the Distributed table replicates data itself. This is worse than using replicated tables, because the consistency of replicas is not checked, and over time they will contain slightly different data.

To select the shard that a row of data is sent to, the sharding expression is analyzed, and its remainder is taken from dividing it by the total weight of the shards. The row is sent to the shard that corresponds to the half-interval of the remainders from `prev_weight` to `prev_weights + weight`, where `prev_weights` is the total weight of the shards with the smallest number, and `weight` is the weight of this shard. For example, if there are two shards, and the first has a weight of 9 while the second has a weight of 10, the row will be sent to the first shard for the remainders from the range \[0, 9), and to the second for the remainders from the range \[9, 19).

The sharding expression can be any expression from constants and table columns that returns an integer. For example, you can use the expression `rand()` for random distribution of data, or `UserID` for distribution by the remainder from dividing the user’s ID (then the data of a single user will reside on a single shard, which simplifies running IN and JOIN by users). If one of the columns is not distributed evenly enough, you can wrap it in a hash function: intHash64(UserID).

A simple reminder from the division is a limited solution for sharding and isn’t always appropriate. It works for medium and large volumes of data (dozens of servers), but not for very large volumes of data (hundreds of servers or more). In the latter case, use the sharding scheme required by the subject area, rather than using entries in Distributed tables.

SELECT queries are sent to all the shards and work regardless of how data is distributed across the shards (they can be distributed completely randomly). When you add a new shard, you don’t have to transfer the old data to it. You can write new data with a heavier weight – the data will be distributed slightly unevenly, but queries will work correctly and efficiently.

You should be concerned about the sharding scheme in the following cases:

-   Queries are used that require joining data (IN or JOIN) by a specific key. If data is sharded by this key, you can use local IN or JOIN instead of GLOBAL IN or GLOBAL JOIN, which is much more efficient.
-   A large number of servers is used (hundreds or more) with a large number of small queries (queries of individual clients - websites, advertisers, or partners). In order for the small queries to not affect the entire cluster, it makes sense to locate data for a single client on a single shard. Alternatively, as we’ve done in Yandex.Metrica, you can set up bi-level sharding: divide the entire cluster into “layers”, where a layer may consist of multiple shards. Data for a single client is located on a single layer, but shards can be added to a layer as necessary, and data is randomly distributed within them. Distributed tables are created for each layer, and a single shared distributed table is created for global queries.

Data is written asynchronously. When inserted in the table, the data block is just written to the local file system. The data is sent to the remote servers in the background as soon as possible. The period for sending data is managed by the [distributed\_directory\_monitor\_sleep\_time\_ms](../../../operations/settings/settings.md#distributed_directory_monitor_sleep_time_ms) and [distributed\_directory\_monitor\_max\_sleep\_time\_ms](../../../operations/settings/settings.md#distributed_directory_monitor_max_sleep_time_ms) settings. The `Distributed` engine sends each file with inserted data separately, but you can enable batch sending of files with the [distributed\_directory\_monitor\_batch\_inserts](../../../operations/settings/settings.md#distributed_directory_monitor_batch_inserts) setting. This setting improves cluster performance by better utilizing local server and network resources. You should check whether data is sent successfully by checking the list of files (data waiting to be sent) in the table directory: `/var/lib/clickhouse/data/database/table/`. The number of threads performing background tasks can be set by [background\_distributed\_schedule\_pool\_size](../../../operations/settings/settings.md#background_distributed_schedule_pool_size) setting.

If the server ceased to exist or had a rough restart (for example, after a device failure) after an INSERT to a Distributed table, the inserted data might be lost. If a damaged data part is detected in the table directory, it is transferred to the `broken` subdirectory and no longer used.

When the `max_parallel_replicas` option is enabled, query processing is parallelized across all replicas within a single shard. For more information, see the section [max\_parallel\_replicas](../../../operations/settings/settings.md#settings-max_parallel_replicas).

## Virtual Columns {#virtual-columns}

-   `_shard_num` — Contains the `shard_num` (from `system.clusters`). Type: [UInt32](../../../sql-reference/data-types/int-uint.md).

!!! note "Note"
    Since [`remote`](../../../sql-reference/table-functions/remote.md)/`cluster` table functions internally create temporary instance of the same Distributed engine, `_shard_num` is available there too.

**See Also**

-   [Virtual columns](../../../engines/table-engines/special/index.md#table_engines-virtual_columns)
-   [background\_distributed\_schedule\_pool\_size](../../../operations/settings/settings.md#background_distributed_schedule_pool_size)

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/distributed/) <!--hide-->
