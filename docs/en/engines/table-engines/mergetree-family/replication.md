---
sidebar_position: 20
sidebar_label: Data Replication
---

# Data Replication {#table_engines-replication}

Replication is only supported for tables in the MergeTree family:

-   ReplicatedMergeTree
-   ReplicatedSummingMergeTree
-   ReplicatedReplacingMergeTree
-   ReplicatedAggregatingMergeTree
-   ReplicatedCollapsingMergeTree
-   ReplicatedVersionedCollapsingMergeTree
-   ReplicatedGraphiteMergeTree

Replication works at the level of an individual table, not the entire server. A server can store both replicated and non-replicated tables at the same time.

Replication does not depend on sharding. Each shard has its own independent replication.

Compressed data for `INSERT` and `ALTER` queries is replicated (for more information, see the documentation for [ALTER](../../../sql-reference/statements/alter/index.md#query_language_queries_alter)).

`CREATE`, `DROP`, `ATTACH`, `DETACH` and `RENAME` queries are executed on a single server and are not replicated:

-   The `CREATE TABLE` query creates a new replicatable table on the server where the query is run. If this table already exists on other servers, it adds a new replica.
-   The `DROP TABLE` query deletes the replica located on the server where the query is run.
-   The `RENAME` query renames the table on one of the replicas. In other words, replicated tables can have different names on different replicas.

ClickHouse uses [Apache ZooKeeper](https://zookeeper.apache.org) for storing replicas meta information. Use ZooKeeper version 3.4.5 or newer.

To use replication, set parameters in the [zookeeper](../../../operations/server-configuration-parameters/settings.md#server-settings_zookeeper) server configuration section.

:::warning
Don’t neglect the security setting. ClickHouse supports the `digest` [ACL scheme](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) of the ZooKeeper security subsystem.
:::

Example of setting the addresses of the ZooKeeper cluster:

``` xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <node>
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

ClickHouse also supports to store replicas meta information in the auxiliary ZooKeeper cluster by providing ZooKeeper cluster name and path as engine arguments.
In other word, it supports to store the metadata of differnt tables in different ZooKeeper clusters.

Example of setting the addresses of the auxiliary ZooKeeper cluster:

``` xml
<auxiliary_zookeepers>
    <zookeeper2>
        <node>
            <host>example_2_1</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_2</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_3</host>
            <port>2181</port>
        </node>
    </zookeeper2>
    <zookeeper3>
        <node>
            <host>example_3_1</host>
            <port>2181</port>
        </node>
    </zookeeper3>
</auxiliary_zookeepers>
```

To store table datameta in a auxiliary ZooKeeper cluster instead of default ZooKeeper cluster, we can use the SQL to create table with
ReplicatedMergeTree engine as follow:

```
CREATE TABLE table_name ( ... ) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```
You can specify any existing ZooKeeper cluster and the system will use a directory on it for its own data (the directory is specified when creating a replicatable table).

If ZooKeeper isn’t set in the config file, you can’t create replicated tables, and any existing replicated tables will be read-only.

ZooKeeper is not used in `SELECT` queries because replication does not affect the performance of `SELECT` and queries run just as fast as they do for non-replicated tables. When querying distributed replicated tables, ClickHouse behavior is controlled by the settings [max_replica_delay_for_distributed_queries](../../../operations/settings/settings.md#settings-max_replica_delay_for_distributed_queries) and [fallback_to_stale_replicas_for_distributed_queries](../../../operations/settings/settings.md#settings-fallback_to_stale_replicas_for_distributed_queries).

For each `INSERT` query, approximately ten entries are added to ZooKeeper through several transactions. (To be more precise, this is for each inserted block of data; an INSERT query contains one block or one block per `max_insert_block_size = 1048576` rows.) This leads to slightly longer latencies for `INSERT` compared to non-replicated tables. But if you follow the recommendations to insert data in batches of no more than one `INSERT` per second, it does not create any problems. The entire ClickHouse cluster used for coordinating one ZooKeeper cluster has a total of several hundred `INSERTs` per second. The throughput on data inserts (the number of rows per second) is just as high as for non-replicated data.

For very large clusters, you can use different ZooKeeper clusters for different shards. However, from our experience this has not proven necessary based on production clusters with approximately 300 servers.

Replication is asynchronous and multi-master. `INSERT` queries (as well as `ALTER`) can be sent to any available server. Data is inserted on the server where the query is run, and then it is copied to the other servers. Because it is asynchronous, recently inserted data appears on the other replicas with some latency. If part of the replicas are not available, the data is written when they become available. If a replica is available, the latency is the amount of time it takes to transfer the block of compressed data over the network. The number of threads performing background tasks for replicated tables can be set by [background_schedule_pool_size](../../../operations/settings/settings.md#background_schedule_pool_size) setting.

`ReplicatedMergeTree` engine uses a separate thread pool for replicated fetches. Size of the pool is limited by the [background_fetches_pool_size](../../../operations/settings/settings.md#background_fetches_pool_size) setting which can be tuned with a server restart.

By default, an INSERT query waits for confirmation of writing the data from only one replica. If the data was successfully written to only one replica and the server with this replica ceases to exist, the stored data will be lost. To enable getting confirmation of data writes from multiple replicas, use the `insert_quorum` option.

Each block of data is written atomically. The INSERT query is divided into blocks up to `max_insert_block_size = 1048576` rows. In other words, if the `INSERT` query has less than 1048576 rows, it is made atomically.

Data blocks are deduplicated. For multiple writes of the same data block (data blocks of the same size containing the same rows in the same order), the block is only written once. The reason for this is in case of network failures when the client application does not know if the data was written to the DB, so the `INSERT` query can simply be repeated. It does not matter which replica INSERTs were sent to with identical data. `INSERTs` are idempotent. Deduplication parameters are controlled by [merge_tree](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-merge_tree) server settings.

During replication, only the source data to insert is transferred over the network. Further data transformation (merging) is coordinated and performed on all the replicas in the same way. This minimizes network usage, which means that replication works well when replicas reside in different datacenters. (Note that duplicating data in different datacenters is the main goal of replication.)

You can have any number of replicas of the same data. Based on our experiences, a relatively reliable and convenient solution could use double replication in production, with each server using RAID-5 or RAID-6 (and RAID-10 in some cases). 

The system monitors data synchronicity on replicas and is able to recover after a failure. Failover is automatic (for small differences in data) or semi-automatic (when data differs too much, which may indicate a configuration error).

## Creating Replicated Tables {#creating-replicated-tables}

The `Replicated` prefix is added to the table engine name. For example:`ReplicatedMergeTree`.

**Replicated\*MergeTree parameters**

-   `zoo_path` — The path to the table in ZooKeeper.
-   `replica_name` — The replica name in ZooKeeper.
-   `other_parameters` — Parameters of an engine which is used for creating the replicated version, for example, version in `ReplacingMergeTree`.

Example:

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">

<summary>Example in deprecated syntax</summary>

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
```

</details>

As the example shows, these parameters can contain substitutions in curly brackets. The substituted values are taken from the [macros](../../../operations/server-configuration-parameters/settings.md#macros) section of the configuration file.

Example:

``` xml
<macros>
    <layer>05</layer>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

The path to the table in ZooKeeper should be unique for each replicated table. Tables on different shards should have different paths.
In this case, the path consists of the following parts:

`/clickhouse/tables/` is the common prefix. We recommend using exactly this one.

`{layer}-{shard}` is the shard identifier. In this example it consists of two parts, since the example cluster uses bi-level sharding. For most tasks, you can leave just the {shard} substitution, which will be expanded to the shard identifier.

`table_name` is the name of the node for the table in ZooKeeper. It is a good idea to make it the same as the table name. It is defined explicitly, because in contrast to the table name, it does not change after a RENAME query.
*HINT*: you could add a database name in front of `table_name` as well. E.g. `db_name.table_name`

The two built-in substitutions `{database}` and `{table}` can be used, they expand into the table name and the database name respectively (unless these macros are defined in the `macros` section). So the zookeeper path can be specified as `'/clickhouse/tables/{layer}-{shard}/{database}/{table}'`.
Be careful with table renames when using these built-in substitutions. The path in Zookeeper cannot be changed, and when the table is renamed, the macros will expand into a different path, the table will refer to a path that does not exist in Zookeeper, and will go into read-only mode.

The replica name identifies different replicas of the same table. You can use the server name for this, as in the example. The name only needs to be unique within each shard.

You can define the parameters explicitly instead of using substitutions. This might be convenient for testing and for configuring small clusters. However, you can’t use distributed DDL queries (`ON CLUSTER`) in this case.

When working with large clusters, we recommend using substitutions because they reduce the probability of error.

You can specify default arguments for `Replicated` table engine in the server configuration file. For instance:

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

In this case, you can omit arguments when creating tables:

``` sql
CREATE TABLE table_name (
	x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

It is equivalent to:

``` sql
CREATE TABLE table_name (
	x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

Run the `CREATE TABLE` query on each replica. This query creates a new replicated table, or adds a new replica to an existing one.

If you add a new replica after the table already contains some data on other replicas, the data will be copied from the other replicas to the new one after running the query. In other words, the new replica syncs itself with the others.

To delete a replica, run `DROP TABLE`. However, only one replica is deleted – the one that resides on the server where you run the query.

## Recovery After Failures {#recovery-after-failures}

If ZooKeeper is unavailable when a server starts, replicated tables switch to read-only mode. The system periodically attempts to connect to ZooKeeper.

If ZooKeeper is unavailable during an `INSERT`, or an error occurs when interacting with ZooKeeper, an exception is thrown.

After connecting to ZooKeeper, the system checks whether the set of data in the local file system matches the expected set of data (ZooKeeper stores this information). If there are minor inconsistencies, the system resolves them by syncing data with the replicas.

If the system detects broken data parts (with the wrong size of files) or unrecognized parts (parts written to the file system but not recorded in ZooKeeper), it moves them to the `detached` subdirectory (they are not deleted). Any missing parts are copied from the replicas.

Note that ClickHouse does not perform any destructive actions such as automatically deleting a large amount of data.

When the server starts (or establishes a new session with ZooKeeper), it only checks the quantity and sizes of all files. If the file sizes match but bytes have been changed somewhere in the middle, this is not detected immediately, but only when attempting to read the data for a `SELECT` query. The query throws an exception about a non-matching checksum or size of a compressed block. In this case, data parts are added to the verification queue and copied from the replicas if necessary.

If the local set of data differs too much from the expected one, a safety mechanism is triggered. The server enters this in the log and refuses to launch. The reason for this is that this case may indicate a configuration error, such as if a replica on a shard was accidentally configured like a replica on a different shard. However, the thresholds for this mechanism are set fairly low, and this situation might occur during normal failure recovery. In this case, data is restored semi-automatically - by “pushing a button”.

To start recovery, create the node `/path_to_table/replica_name/flags/force_restore_data` in ZooKeeper with any content, or run the command to restore all replicated tables:

``` bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

Then restart the server. On start, the server deletes these flags and starts recovery.

## Recovery After Complete Data Loss {#recovery-after-complete-data-loss}

If all data and metadata disappeared from one of the servers, follow these steps for recovery:

1.  Install ClickHouse on the server. Define substitutions correctly in the config file that contains the shard identifier and replicas, if you use them.
2.  If you had unreplicated tables that must be manually duplicated on the servers, copy their data from a replica (in the directory `/var/lib/clickhouse/data/db_name/table_name/`).
3.  Copy table definitions located in `/var/lib/clickhouse/metadata/` from a replica. If a shard or replica identifier is defined explicitly in the table definitions, correct it so that it corresponds to this replica. (Alternatively, start the server and make all the `ATTACH TABLE` queries that should have been in the .sql files in `/var/lib/clickhouse/metadata/`.)
4.  To start recovery, create the ZooKeeper node `/path_to_table/replica_name/flags/force_restore_data` with any content, or run the command to restore all replicated tables: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

Then start the server (restart, if it is already running). Data will be downloaded from replicas.

An alternative recovery option is to delete information about the lost replica from ZooKeeper (`/path_to_table/replica_name`), then create the replica again as described in “[Creating replicated tables](#creating-replicated-tables)”.

There is no restriction on network bandwidth during recovery. Keep this in mind if you are restoring many replicas at once.

## Converting from MergeTree to ReplicatedMergeTree {#converting-from-mergetree-to-replicatedmergetree}

We use the term `MergeTree` to refer to all table engines in the `MergeTree family`, the same as for `ReplicatedMergeTree`.

If you had a `MergeTree` table that was manually replicated, you can convert it to a replicated table. You might need to do this if you have already collected a large amount of data in a `MergeTree` table and now you want to enable replication.

If the data differs on various replicas, first sync it, or delete this data on all the replicas except one.

Rename the existing MergeTree table, then create a `ReplicatedMergeTree` table with the old name.
Move the data from the old table to the `detached` subdirectory inside the directory with the new table data (`/var/lib/clickhouse/data/db_name/table_name/`).
Then run `ALTER TABLE ATTACH PARTITION` on one of the replicas to add these data parts to the working set.

## Converting from ReplicatedMergeTree to MergeTree {#converting-from-replicatedmergetree-to-mergetree}

Create a MergeTree table with a different name. Move all the data from the directory with the `ReplicatedMergeTree` table data to the new table’s data directory. Then delete the `ReplicatedMergeTree` table and restart the server.

If you want to get rid of a `ReplicatedMergeTree` table without launching the server:

-   Delete the corresponding `.sql` file in the metadata directory (`/var/lib/clickhouse/metadata/`).
-   Delete the corresponding path in ZooKeeper (`/path_to_table/replica_name`).

After this, you can launch the server, create a `MergeTree` table, move the data to its directory, and then restart the server.

## Recovery When Metadata in the Zookeeper Cluster Is Lost or Damaged {#recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged}

If the data in ZooKeeper was lost or damaged, you can save data by moving it to an unreplicated table as described above.

**See Also**

-   [background_schedule_pool_size](../../../operations/settings/settings.md#background_schedule_pool_size)
-   [background_fetches_pool_size](../../../operations/settings/settings.md#background_fetches_pool_size)
-   [execute_merges_on_single_replica_time_threshold](../../../operations/settings/settings.md#execute-merges-on-single-replica-time-threshold)
-   [max_replicated_fetches_network_bandwidth](../../../operations/settings/merge-tree-settings.md#max_replicated_fetches_network_bandwidth)
-   [max_replicated_sends_network_bandwidth](../../../operations/settings/merge-tree-settings.md#max_replicated_sends_network_bandwidth)

[Original article](https://clickhouse.com/docs/en/operations/table_engines/replication/) <!--hide-->
