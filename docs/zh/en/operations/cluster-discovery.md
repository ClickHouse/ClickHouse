---
description: 'ClickHouse 集群发现文档'
sidebar_label: '集群发现'
slug: /operations/cluster-discovery
title: '集群发现'
doc_type: 'guide'
---

<div id="overview">
  ## 概述
</div>

ClickHouse 的 Cluster Discovery 功能可让节点自动发现并完成注册，无需在配置文件中逐一显式定义，从而简化集群配置。这在手动定义每个节点变得繁琐时尤其有用。

:::note

Cluster Discovery 是一项 Experimental 功能，未来版本中可能会更改或移除。
要启用该功能，请在配置文件中加入 `allow_experimental_cluster_discovery` 设置：

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```

:::

<div id="remote-servers-configuration">
  ## 远程服务器配置
</div>

<div id="traditional-manual-configuration">
  ### 传统手动配置
</div>

传统上，在 ClickHouse 中，集群中的每个分片和副本都需要在配置文件中手动指定：

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

<div id="using-cluster-discovery">
  ### 使用集群发现
</div>

借助集群发现，无需显式定义每个节点，只需在 ZooKeeper 中指定一个路径。所有在 ZooKeeper 中注册到该路径下的节点都会被自动发现并加入集群。

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

如果要为特定节点指定分片编号，可以在 `<discovery>` 部分中加入 `<shard>` 标签：

对于 `node1` 和 `node2`：

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

对于 `node3` 和 `node4`：

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

<div id="observer-mode">
  ### 观察者模式
</div>

配置为观察者模式的节点不会将自身注册为副本。
它们只会观察并发现集群中其他处于活动状态的副本，而不会主动参与其中。
要启用观察者模式，请在 `<discovery>` 部分中加入 `<observer/>` 标签：

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```

<div id="discovery-of-clusters">
  ### 集群发现
</div>

有时，你可能不仅需要在集群中添加或移除主机，还需要添加或移除集群本身。你可以使用 `<multicluster_root_path>` 节点，将其作为多个集群的根路径：

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

在这种情况下，当其他主机在路径 `/clickhouse/discovery/some_new_cluster` 下注册自身时，系统会添加一个名为 `some_new_cluster` 的集群。

你可以同时使用这两种功能：主机既可以在集群 `my_cluster` 中注册自身，也可以发现其他任意集群：

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

限制：

* 不能在同一个 `remote_servers` 子树中同时使用 `<path>` 和 `<multicluster_root_path>`。
* `<multicluster_root_path>` 只能与 `<observer/>` 搭配使用。
* 来自 Keeper 的路径最后一部分会用作集群名称，而在注册时，名称取自 XML 标签。

<div id="use-cases-and-limitations">
  ## 用例和限制
</div>

当在指定的 ZooKeeper 路径中添加或移除节点时，无需修改配置或重启服务器，系统就会自动发现这些节点，或将其从集群中移除。

不过，这些变更只会影响集群配置，不会影响数据或现有的数据库和表。

请看下面这个由 3 个节点组成的集群示例：

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

然后，我们向集群中添加一个新节点：在配置文件的 `remote_servers` 部分中为其使用相同的条目并启动该节点：

```response
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

第四个节点已加入集群，但表 `event_table` 仍然只存在于前三个节点上：

```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

如果您需要在所有节点上复制这些表，可以使用 [Replicated](../engines/database-engines/replicated.md) 数据库引擎，作为集群发现的替代方案。