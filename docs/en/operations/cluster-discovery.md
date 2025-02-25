---
slug: /operations/cluster-discovery
sidebar_label: Cluster Discovery
---
# Cluster Discovery

## Overview {#overview}

ClickHouse's Cluster Discovery feature simplifies cluster configuration by allowing nodes to automatically discover and register themselves without the need for explicit definition in the configuration files. This is especially beneficial in cases where the manual definition of each node becomes cumbersome.

:::note

Cluster Discovery is an experimental feature and can be changed or removed in future versions.
To enable it include the `allow_experimental_cluster_discovery` setting in your configuration file:

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```
:::

## Remote Servers Configuration {#remote-servers-configuration}

### Traditional Manual Configuration {#traditional-manual-configuration}

Traditionally, in ClickHouse, each shard and replica in the cluster needed to be manually specified in the configuration:

```xml
<remote_servers>
    <cluster_name>
        <shard>
            <replica>
                <host>node1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>node3</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node4</host>
                <port>9000</port>
            </replica>
        </shard>
    </cluster_name>
</remote_servers>

```

### Using Cluster Discovery {#using-cluster-discovery}

With Cluster Discovery, rather than defining each node explicitly, you simply specify a path in ZooKeeper. All nodes that register under this path in ZooKeeper will be automatically discovered and added to the cluster.

```xml
<remote_servers>
    <cluster_name>
        <discovery>
            <path>/clickhouse/discovery/cluster_name</path>

            <!-- # Optional configuration parameters: -->

            <!-- ## Authentication credentials to access all other nodes in cluster: -->
            <!-- <user>user1</user> -->
            <!-- <password>pass123</password> -->
            <!-- ### Alternatively to password, interserver secret may be used: -->
            <!-- <secret>secret123</secret> -->

            <!-- ## Shard for current node (see below): -->
            <!-- <shard>1</shard> -->

            <!-- ## Observer mode (see below): -->
            <!-- <observer/> -->
        </discovery>
    </cluster_name>
</remote_servers>
```

If you want to specify a shard number for a particular node, you can include the `<shard>` tag within the `<discovery>` section:

for `node1` and `node2`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

for `node3` and `node4`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

### Observer mode {#observer-mode}


Nodes configured in observer mode will not register themselves as replicas.
They will solely observe and discover other active replicas in the cluster without actively participating.
To enable observer mode, include the `<observer/>` tag within the `<discovery>` section:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```


### Discovery of clusters {#discovery-of-clusters}

Sometimes you may need to add and remove not only hosts in clusters, but clusters themselfs. You can use the `<multicluster_root_path>` node with root path for several clusters:

```xml
<remote_servers>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

In this case, when some other host registers itself with the path `/clickhouse/discovery/some_new_cluster`, a cluster with name `some_new_cluster` will be added.

You can use both features simultaneously, the host can register itself in cluster `my_cluster` and discovery any other clusters:

```xml
<remote_servers>
    <my_cluster>
        <discovery>
            <path>/clickhouse/discovery/my_cluster</path>
        </discovery>
    </my_cluster>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

Limitations:
- You can't use both `<path>` and `<multicluster_root_path>` in the same `remote_servers` subtree.
- `<multicluster_root_path>` can only be with `<observer/>`.
- The last part of path from Keeper is used as the cluster name, while during registration the name is taken from the XML tag.



## Use-Cases and Limitations {#use-cases-and-limitations}

As nodes are added or removed from the specified ZooKeeper path, they are automatically discovered or removed from the cluster without the need for configuration changes or server restarts.

However, changes affect only cluster configuration, not the data or existing databases and tables.

Consider the following example with a cluster of 3 nodes:


```xml
<remote_servers>
    <default>
        <discovery>
            <path>/clickhouse/discovery/default_cluster</path>
        </discovery>
    </default>
</remote_servers>
```

```sql
SELECT * EXCEPT (default_database, errors_count, slowdowns_count, estimated_recovery_time, database_shard_name, database_replica_name)
FROM system.clusters WHERE cluster = 'default';

┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

```sql
CREATE TABLE event_table ON CLUSTER default (event_time DateTime, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/event_table', '{replica}')
ORDER BY event_time PARTITION BY toYYYYMM(event_time);

INSERT INTO event_table ...
```

Then, we add a new node to the cluster, starting a new node with the same entry in the `remote_servers` section in a configuration file:

```response
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

The fourth node is participating in the cluster, but table `event_table` still exists only on the first three nodes:


```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

If you need to have tables replicated on all the nodes, you may use the [Replicated](../engines/database-engines/replicated.md) database engine in alternative to cluster discovery.

