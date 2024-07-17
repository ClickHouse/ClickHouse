---
sidebar_label: "Distributed"
sidebar_position: 10
slug: /en/engines/table-engines/special/distributed
---

# Distributed Table Engine

:::warning
To create a distributed table engine in the cloud, you can use the [remote and remoteSecure](../../../sql-reference/table-functions/remote) table functions. The `Distributed(...)` syntax cannot be used in ClickHouse Cloud.
:::

Tables with Distributed engine do not store any data of their own, but allow distributed query processing on multiple servers. Reading is automatically parallelized. During a read, the table indexes on remote servers are used, if there are any.

## Creating a Table {#distributed-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]])
[SETTINGS name=value, ...]
```

### From a Table {#distributed-from-a-table}

When the `Distributed` table is pointing to a table on the current server you can adopt that table's schema:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster] AS [db2.]name2 ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]]) [SETTINGS name=value, ...]
```

### Distributed Parameters

#### cluster

`cluster` - the cluster name in the server’s config file

#### database

`database` - the name of a remote database

#### table

`table` - the name of a remote table

#### sharding_key

`sharding_key` - (optionally) sharding key

Specifying the `sharding_key` is necessary for the following:

- For `INSERTs` into a distributed table (as the table engine needs the `sharding_key` to determine how to split the data). However, if `insert_distributed_one_random_shard` setting is enabled, then `INSERTs` do not need the sharding key.
- For use with `optimize_skip_unused_shards` as the `sharding_key` is necessary to determine what shards should be queried

#### policy_name

`policy_name` - (optionally) policy name, it will be used to store temporary files for background send

**See Also**

 - [distributed_foreground_insert](../../../operations/settings/settings.md#distributed_foreground_insert) setting
 - [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) for the examples

### Distributed Settings

#### fsync_after_insert

`fsync_after_insert` - do the `fsync` for the file data after background insert to Distributed. Guarantees that the OS flushed the whole inserted data to a file **on the initiator node** disk.

#### fsync_directories

`fsync_directories` - do the `fsync` for directories. Guarantees that the OS refreshed directory metadata after operations related to background inserts on Distributed table (after insert, after sending the data to shard, etc.).

#### skip_unavailable_shards

`skip_unavailable_shards` - If true, ClickHouse silently skips unavailable shards. Shard is marked as unavailable when: 1) The shard cannot be reached due to a connection failure. 2) Shard is unresolvable through DNS. 3) Table does not exist on the shard. Default false.

#### bytes_to_throw_insert

`bytes_to_throw_insert` - if more than this number of compressed bytes will be pending for background INSERT, an exception will be thrown. 0 - do not throw. Default 0.

#### bytes_to_delay_insert

`bytes_to_delay_insert` - if more than this number of compressed bytes will be pending for background INSERT, the query will be delayed. 0 - do not delay. Default 0.

#### max_delay_to_insert

`max_delay_to_insert` - max delay of inserting data into Distributed table in seconds, if there are a lot of pending bytes for background send. Default 60.

#### background_insert_batch

`background_insert_batch` - same as [distributed_background_insert_batch](../../../operations/settings/settings.md#distributed_background_insert_batch)

#### background_insert_split_batch_on_failure

`background_insert_split_batch_on_failure` - same as [distributed_background_insert_split_batch_on_failure](../../../operations/settings/settings.md#distributed_background_insert_split_batch_on_failure)

#### background_insert_sleep_time_ms

`background_insert_sleep_time_ms` - same as [distributed_background_insert_sleep_time_ms](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms)

#### background_insert_max_sleep_time_ms

`background_insert_max_sleep_time_ms` - same as [distributed_background_insert_max_sleep_time_ms](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms)

#### flush_on_detach

`flush_on_detach` - Flush data to remote nodes on DETACH/DROP/server shutdown. Default true.

:::note
**Durability settings** (`fsync_...`):

- Affect only background INSERTs (i.e. `distributed_foreground_insert=false`) when data first stored on the initiator node disk and later, in background, send to shards.
- May significantly decrease the inserts' performance
- Affect writing the data stored inside Distributed table folder into the **node which accepted your insert**. If you need to have guarantees of writing data to underlying MergeTree tables - see durability settings (`...fsync...`) in `system.merge_tree_settings`

For **Insert limit settings** (`..._insert`) see also:

- [distributed_foreground_insert](../../../operations/settings/settings.md#distributed_foreground_insert) setting
- [prefer_localhost_replica](../../../operations/settings/settings.md#prefer-localhost-replica) setting
- `bytes_to_throw_insert` handled before `bytes_to_delay_insert`, so you should not set it to the value less then `bytes_to_delay_insert`
:::

**Example**

``` sql
CREATE TABLE hits_all AS hits
ENGINE = Distributed(logs, default, hits[, sharding_key[, policy_name]])
SETTINGS
    fsync_after_insert=0,
    fsync_directories=0;
```

Data will be read from all servers in the `logs` cluster, from the `default.hits` table located on every server in the cluster. Data is not only read but is partially processed on the remote servers (to the extent that this is possible). For example, for a query with `GROUP BY`, data will be aggregated on remote servers, and the intermediate states of aggregate functions will be sent to the requestor server. Then data will be further aggregated.

Instead of the database name, you can use a constant expression that returns a string. For example: `currentDatabase()`.

## Clusters {#distributed-clusters}

Clusters are configured in the [server configuration file](../../../operations/configuration-files.md):

``` xml
<remote_servers>
    <logs>
        <!-- Inter-server per-cluster secret for Distributed queries
             default: no secret (no authentication will be performed)

             If set, then Distributed queries will be validated on shards, so at least:
             - such cluster should exist on the shard,
             - such cluster should have the same secret.

             And also (and which is more important), the initial_user will
             be used as current user for the query.
        -->
        <!-- <secret></secret> -->
        
        <!-- Optional. Whether distributed DDL queries (ON CLUSTER clause) are allowed for this cluster. Default: true (allowed). -->        
        <!-- <allow_distributed_ddl_queries>true</allow_distributed_ddl_queries> -->
        
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

Here a cluster is defined with the name `logs` that consists of two shards, each of which contains two replicas. Shards refer to the servers that contain different parts of the data (in order to read all the data, you must access all the shards). Replicas are duplicating servers (in order to read all the data, you can access the data on any one of the replicas).

Cluster names must not contain dots.

The parameters `host`, `port`, and optionally `user`, `password`, `secure`, `compression` are specified for each server:

- `host` – The address of the remote server. You can use either the domain or the IPv4 or IPv6 address. If you specify the domain, the server makes a DNS request when it starts, and the result is stored as long as the server is running. If the DNS request fails, the server does not start. If you change the DNS record, restart the server.
- `port` – The TCP port for messenger activity (`tcp_port` in the config, usually set to 9000). Not to be confused with `http_port`.
- `user` – Name of the user for connecting to a remote server. Default value is the `default` user. This user must have access to connect to the specified server. Access is configured in the `users.xml` file. For more information, see the section [Access rights](../../../guides/sre/user-management/index.md).
- `password` – The password for connecting to a remote server (not masked). Default value: empty string.
- `secure` - Whether to use a secure SSL/TLS connection. Usually also requires specifying the port (the default secure port is `9440`). The server should listen on `<tcp_port_secure>9440</tcp_port_secure>` and be configured with correct certificates.
- `compression` - Use data compression. Default value: `true`.

When specifying replicas, one of the available replicas will be selected for each of the shards when reading. You can configure the algorithm for load balancing (the preference for which replica to access) – see the [load_balancing](../../../operations/settings/settings.md#load_balancing) setting. If the connection with the server is not established, there will be an attempt to connect with a short timeout. If the connection failed, the next replica will be selected, and so on for all the replicas. If the connection attempt failed for all the replicas, the attempt will be repeated the same way, several times. This works in favour of resiliency, but does not provide complete fault tolerance: a remote server might accept the connection, but might not work, or work poorly.

You can specify just one of the shards (in this case, query processing should be called remote, rather than distributed) or up to any number of shards. In each shard, you can specify from one to any number of replicas. You can specify a different number of replicas for each shard.

You can specify as many clusters as you wish in the configuration.

To view your clusters, use the `system.clusters` table.

The `Distributed` engine allows working with a cluster like a local server. However, the cluster's configuration cannot be specified dynamically, it has to be configured in the server config file. Usually, all servers in a cluster will have the same cluster config (though this is not required). Clusters from the config file are updated on the fly, without restarting the server.

If you need to send a query to an unknown set of shards and replicas each time, you do not need to create a `Distributed` table – use the `remote` table function instead. See the section [Table functions](../../../sql-reference/table-functions/index.md).

## Writing data {#distributed-writing-data}

There are two methods for writing data to a cluster:

First, you can define which servers to write which data to and perform the write directly on each shard. In other words, perform direct `INSERT` statements on the remote tables in the cluster that the `Distributed` table is pointing to. This is the most flexible solution as you can use any sharding scheme, even one that is non-trivial due to the requirements of the subject area. This is also the most optimal solution since data can be written to different shards completely independently.

Second, you can perform `INSERT` statements on a `Distributed` table. In this case, the table will distribute the inserted data across the servers itself. In order to write to a `Distributed` table, it must have the `sharding_key` parameter configured (except if there is only one shard).

Each shard can have a `<weight>` defined in the config file. By default, the weight is `1`. Data is distributed across shards in the amount proportional to the shard weight. All shard weights are summed up, then each shard's weight is divided by the total to determine each shard's proportion. For example, if there are two shards and the first has a weight of 1 while the second has a weight of 2, the first will be sent one third (1 / 3) of inserted rows and the second will be sent two thirds (2 / 3).

Each shard can have the `internal_replication` parameter defined in the config file. If this parameter is set to `true`, the write operation selects the first healthy replica and writes data to it. Use this if the tables underlying the `Distributed` table are replicated tables (e.g. any of the `Replicated*MergeTree` table engines). One of the table replicas will receive the write, and it will be replicated to the other replicas automatically.

If `internal_replication` is set to `false` (the default), data is written to all replicas. In this case, the `Distributed` table replicates data itself. This is worse than using replicated tables because the consistency of replicas is not checked and, over time, they will contain slightly different data.

To select the shard that a row of data is sent to, the sharding expression is analyzed, and its remainder is taken from dividing it by the total weight of the shards. The row is sent to the shard that corresponds to the half-interval of the remainders from `prev_weights` to `prev_weights + weight`, where `prev_weights` is the total weight of the shards with the smallest number, and `weight` is the weight of this shard. For example, if there are two shards, and the first has a weight of 9 while the second has a weight of 10, the row will be sent to the first shard for the remainders from the range \[0, 9), and to the second for the remainders from the range \[9, 19).

The sharding expression can be any expression from constants and table columns that returns an integer. For example, you can use the expression `rand()` for random distribution of data, or `UserID` for distribution by the remainder from dividing the user’s ID (then the data of a single user will reside on a single shard, which simplifies running `IN` and `JOIN` by users). If one of the columns is not distributed evenly enough, you can wrap it in a hash function e.g. `intHash64(UserID)`.

A simple remainder from the division is a limited solution for sharding and isn’t always appropriate. It works for medium and large volumes of data (dozens of servers), but not for very large volumes of data (hundreds of servers or more). In the latter case, use the sharding scheme required by the subject area rather than using entries in `Distributed` tables.

You should be concerned about the sharding scheme in the following cases:

- Queries are used that require joining data (`IN` or `JOIN`) by a specific key. If data is sharded by this key, you can use local `IN` or `JOIN` instead of `GLOBAL IN` or `GLOBAL JOIN`, which is much more efficient.
- A large number of servers is used (hundreds or more) with a large number of small queries, for example, queries for data of individual clients (e.g. websites, advertisers, or partners). In order for the small queries to not affect the entire cluster, it makes sense to locate data for a single client on a single shard. Alternatively, you can set up bi-level sharding: divide the entire cluster into “layers”, where a layer may consist of multiple shards. Data for a single client is located on a single layer, but shards can be added to a layer as necessary, and data is randomly distributed within them. `Distributed` tables are created for each layer, and a single shared distributed table is created for global queries.

Data is written in background. When inserted in the table, the data block is just written to the local file system. The data is sent to the remote servers in the background as soon as possible. The periodicity for sending data is managed by the [distributed_background_insert_sleep_time_ms](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) and [distributed_background_insert_max_sleep_time_ms](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms) settings. The `Distributed` engine sends each file with inserted data separately, but you can enable batch sending of files with the [distributed_background_insert_batch](../../../operations/settings/settings.md#distributed_background_insert_batch) setting. This setting improves cluster performance by better utilizing local server and network resources. You should check whether data is sent successfully by checking the list of files (data waiting to be sent) in the table directory: `/var/lib/clickhouse/data/database/table/`. The number of threads performing background tasks can be set by [background_distributed_schedule_pool_size](../../../operations/settings/settings.md#background_distributed_schedule_pool_size) setting.

If the server ceased to exist or had a rough restart (for example, due to a hardware failure) after an `INSERT` to a `Distributed` table, the inserted data might be lost. If a damaged data part is detected in the table directory, it is transferred to the `broken` subdirectory and no longer used.

## Reading data {#distributed-reading-data}

When querying a `Distributed` table, `SELECT` queries are sent to all shards and work regardless of how data is distributed across the shards (they can be distributed completely randomly). When you add a new shard, you do not have to transfer old data into it. Instead, you can write new data to it by using a heavier weight – the data will be distributed slightly unevenly, but queries will work correctly and efficiently.

When the `max_parallel_replicas` option is enabled, query processing is parallelized across all replicas within a single shard. For more information, see the section [max_parallel_replicas](../../../operations/settings/settings.md#max_parallel_replicas).

To learn more about how distributed `in` and `global in` queries are processed, refer to [this](../../../sql-reference/operators/in.md#select-distributed-subqueries) documentation.

## Virtual Columns {#virtual-columns}

#### _shard_num

`_shard_num` — Contains the `shard_num` value from the table `system.clusters`. Type: [UInt32](../../../sql-reference/data-types/int-uint.md).

:::note
Since [remote](../../../sql-reference/table-functions/remote.md) and [cluster](../../../sql-reference/table-functions/cluster.md) table functions internally create temporary Distributed table, `_shard_num` is available there too.
:::

**See Also**

- [Virtual columns](../../../engines/table-engines/index.md#table_engines-virtual_columns) description
- [background_distributed_schedule_pool_size](../../../operations/settings/settings.md#background_distributed_schedule_pool_size) setting
- [shardNum()](../../../sql-reference/functions/other-functions.md#shardnum) and [shardCount()](../../../sql-reference/functions/other-functions.md#shardcount) functions
