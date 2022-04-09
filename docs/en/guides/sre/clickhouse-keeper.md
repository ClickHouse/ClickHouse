---
sidebar_label: Configuring ClickHouse Keeper
sidebar_position: 20
---

# Configuring ClickHouse Keeper 

ClickHouse Keeper is a component included in ClickHouse to handle replication and coordinated operations across nodes and clusters.
This part of the system replaces the requirement of having a separate Zookeper installation and is compatible with Zookeper for ClickHouse operations.

This guide provides simple and minimal settings to configure ClicKHouse Keeper with an example on how to test distributed operations. This example is performed using 3 nodes on Linux.


## 1. Configure Nodes with Keeper settings

1. Install 3 ClickHouse instances on 3 hosts (chnode1, chnode2, chnode3). (View the [Quick Start](../../quick-start.mdx) for details on installing ClickHouse.)

2. On each node, add the following entry to allow external communication through the network interface.
    ```xml
    <listen_host>0.0.0.0</listen_host>
    ```

3. Add the following ClickHouse Keeper configuration to all three servers updating the `<server_id>` setting for each server; for `chnode1` would be `1`, `chnode2` would be `2`, etc.
    ```xml
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>

        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>chnode1.domain.com</hostname>
                <port>9444</port>
            </server>
            <server>
                <id>2</id>
                <hostname>chnode2.domain.com</hostname>
                <port>9444</port>
            </server>
            <server>
                <id>3</id>
                <hostname>chnode3.domain.com</hostname>
                <port>9444</port>
            </server>
        </raft_configuration>
    </keeper_server>
    ```

    These are the basic settings used above:

    |Parameter |Description                   |Example              |
    |----------|------------------------------|---------------------|
    |tcp_port   |port to be used by clients of keeper|9181 default equivalent of 2181 as in zookeeper|
    |server_id| identifier for each CLickHouse Keeper server used in raft configuration| 1|
    |coordination_settings| section to parameters such as timeouts| timeouts: 10000, log level: trace|
    |server    |definition of server participating|list of each server definition|
    |raft_configuration| settings for each server in the keeper cluster| server and settings for each|
    |id      |numeric id of the server for keeper services|1|
    |hostname   |hostname, IP or FQDN of each server in the keeper cluster|chnode1.domain.com|
    |port|port to listen on for interserver keeper connections|9444|

    :::note
      View the [ClickHouse Keeper docs page](../../en/operations/clickhouse-keeper.md) for details on all the available parameters.
    :::


4.  Enable the Zookeeper component. It will use the ClickHouse Keeper engine:
    ```xml
        <zookeeper>
            <node>
                <host>chnode1.domain.com</host>
                <port>9181</port>
            </node>
            <node>
                <host>chnode2.domain.com</host>
                <port>9181</port>
            </node>
            <node>
                <host>chnode3.domain.com</host>
                <port>9181</port>
            </node>
        </zookeeper>
    ```

    These are the basic settings used above:

    |Parameter |Description                   |Example              |
    |----------|------------------------------|---------------------|
    |node   |list of nodes for ClickHouse Keeper connections|settings entry for each server|
    |host|hostname, IP or FQDN of each ClickHouse keepr node| chnode1.domain.com|
    |port|ClickHouse Keeper client port| 9181|

5. Restart ClickHouse and verify that each Keeper instance is running. Execute the following command on each server. The `ruok` command returns `imok` if Keeper is running and healthy:
    ```bash
    # echo ruok | nc localhost 9181; echo
    imok
    ```

6. The `system` database has a table named `zookeeper` that contains the details of your ClickHouse Keeper instances. Let's view the table:
    ```sql
    SELECT *
    FROM system.zookeeper
    WHERE path IN ('/', '/clickhouse')
    ```

    The table looks like:
    ```response
    ┌─name───────┬─value─┬─czxid─┬─mzxid─┬───────────────ctime─┬───────────────mtime─┬─version─┬─cversion─┬─aversion─┬─ephemeralOwner─┬─dataLength─┬─numChildren─┬─pzxid─┬─path────────┐
    │ clickhouse │       │   124 │   124 │ 2022-03-07 00:49:34 │ 2022-03-07 00:49:34 │       0 │        2 │        0 │              0 │          0 │           2 │  5693 │ /           │
    │ task_queue │       │   125 │   125 │ 2022-03-07 00:49:34 │ 2022-03-07 00:49:34 │       0 │        1 │        0 │              0 │          0 │           1 │   126 │ /clickhouse │
    │ tables     │       │  5693 │  5693 │ 2022-03-07 00:49:34 │ 2022-03-07 00:49:34 │       0 │        3 │        0 │              0 │          0 │           3 │  6461 │ /clickhouse │
    └────────────┴───────┴───────┴───────┴─────────────────────┴─────────────────────┴─────────┴──────────┴──────────┴────────────────┴────────────┴─────────────┴───────┴─────────────┘
    ```


## 2.  Configure a cluster in ClickHouse

1. Let's configure a simple cluster with 2 shards and only one replica on 2 of the nodes. The third node will be used to achieve a quorum for the requirement in ClickHouse Keeper. Update the configuration on `chnode1` and `chnode2`. The following cluster defines 1 shard on each node for a total of 2 shards with no replication. In this example, some of the data will be on node and some will be on the other node:
    ```xml
        <cluster_2S_1R>
            <shard>
                <replica>
                    <host>chnode1.domain.com</host>
                    <port>9000</port>
                    <user>default</user>
                    <password>ClickHouse123!</password>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>chnode2.domain.com</host>
                    <port>9000</port>
                    <user>default</user>
                    <password>ClickHouse123!</password>
                </replica>
            </shard>
        </cluster_2S_1R>
    ```

    |Parameter |Description                   |Example              |
    |----------|------------------------------|---------------------|
    |shard   |list of replicas on the cluster definition|list of replicas for each shard|
    |replica|list of settings for each replica|settings entries for each replica|
    |host|hostname, IP or FQDN of server that will host a replica shard|chnode1.domain.com|
    |port|port used to communicate using the native tcp protocol|9000|
    |user|username that will be used to authenticate to the cluster instances|default|
    |password|password for the user define to allow connections to cluster instances|ClickHouse123!|


2. Restart ClickHouse and verify the cluster was created:
    ```bash
    SHOW clusters;
    ```

    You should see your cluster:
    ```response
    ┌─cluster───────┐
    │ cluster_1S_2R │
    └───────────────┘
    ```

## 3. Create and test distributed table

1.  Create a new database on the new cluster using ClickHouse client on `chnode1`. The `ON CLUSTER` clause automatically creates the database on both nodes.
    ```sql
    CREATE DATABASE db1 ON CLUSTER 'cluster_2S_1R';
    ```

2. Create a new table on the `db1` database. Once again, `ON CLUSTER` creates the table on both nodes.
    ```sql
    CREATE TABLE db1.table1 on cluster 'cluster_2S_1R'
    (
        `id` UInt64,
        `column1` String
    )
    ENGINE = MergeTree
    ORDER BY column1
    ```

3. On the `chnode1` node, add a couple of rows:
    ```sql
    INSERT INTO db1.table1
        (id, column1)
    VALUES
        (1, 'abc'),
        (2, 'def')
    ```

4. Add a couple of rows on the `chnode2` node:
    ```sql
    INSERT INTO db1.table1
        (id, column1)
    VALUES
        (3, 'ghi'),
        (4, 'jkl')
    ```

5. Notice that running a `SELECT` statement on each node only shows the data on that node. For example, on `chnode1`:
    ```sql
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: 7ef1edbc-df25-462b-a9d4-3fe6f9cb0b6d

    ┌─id─┬─column1─┐
    │  1 │ abc     │
    │  2 │ def     │
    └────┴─────────┘

    2 rows in set. Elapsed: 0.006 sec.
    ```

    On `chnode2`:
    ```
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: c43763cc-c69c-4bcc-afbe-50e764adfcbf

    ┌─id─┬─column1─┐
    │  3 │ ghi     │
    │  4 │ jkl     │
    └────┴─────────┘
    ```

6. You can create a `Distributed` table to represent the data on the two shards. Tables with the `Distributed` table engine do not store any data of their own, but allow distributed query processing on multiple servers. Reads hit all the shards, and writes can be distributed across the shards. Run the following query on `chnode1`:
    ```sql
    CREATE TABLE db1.dist_table (
        id UInt64,
        column1 String
    )
    ENGINE = Distributed(cluster_2S_1R,db1,table1)
    ```

7. Notice querying `dist_table` returns all four rows of data from the two shards:
    ```sql
    SELECT *
    FROM db1.dist_table
    ```

    ```response
    Query id: 495bffa0-f849-4a0c-aeea-d7115a54747a

    ┌─id─┬─column1─┐
    │  1 │ abc     │
    │  2 │ def     │
    └────┴─────────┘
    ┌─id─┬─column1─┐
    │  3 │ ghi     │
    │  4 │ jkl     │
    └────┴─────────┘

    4 rows in set. Elapsed: 0.018 sec.
    ```

## Summary

This guide demostrated how to setup a cluster using ClickHouse Keeper. With ClickHouse Keeper, you can configure clusters and define distributed tables that can be replicated across shards.
