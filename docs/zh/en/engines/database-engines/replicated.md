---
description: '该引擎基于 Atomic 引擎。它支持通过将 DDL 日志写入 ZooKeeper，并在给定数据库的所有副本上执行这些日志，来复制元数据。'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'reference'
---

该引擎基于 [Atomic](../../engines/database-engines/atomic.md) 引擎。它支持通过将 DDL 日志写入 ZooKeeper，并在给定数据库的所有副本上执行这些日志，来复制元数据。

一个 ClickHouse server 可以同时运行并更新多个 Replicated 数据库。但同一个 Replicated 数据库不能有多个副本。

<div id="creating-a-database">
  ## 创建数据库
</div>

```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**引擎参数**

* `zoo_path` — ZooKeeper 路径。相同的 ZooKeeper 路径对应同一个数据库。
* `shard_name` — 分片名称。数据库副本按 `shard_name` 分组到各个分片中。
* `replica_name` — 副本名称。同一分片中的所有副本，其名称必须各不相同。

这些参数可以省略，此时缺失的参数将使用默认值。

如果 `zoo_path` 包含宏 `{uuid}`，则必须显式指定 UUID，或在 CREATE 语句中添加 [ON CLUSTER](../../sql-reference/distributed-ddl.md)，以确保该数据库的所有副本使用相同的 UUID。

对于 [ReplicatedMergeTree](/zh/engines/table-engines/mergetree-family/replication) 表，如果未提供参数，则会使用默认参数：`/clickhouse/tables/{uuid}/{shard}` 和 `{replica}`。这些值可以在服务器设置 [default&#95;replica&#95;path](../../operations/server-configuration-parameters/settings.md#default_replica_path) 和 [default&#95;replica&#95;name](../../operations/server-configuration-parameters/settings.md#default_replica_name) 中修改。宏 `{uuid}` 会展开为表的 uuid，`{shard}` 和 `{replica}` 会展开为服务器配置中的值，而不是数据库引擎参数中的值。不过，未来将支持使用 Replicated 数据库的 `shard_name` 和 `replica_name`。

还支持使用辅助 ZooKeeper 集群来存储 Replicated 数据库的元数据，而不是使用默认的 ZooKeeper 集群。我们可以使用如下 SQL，通过辅助 ZooKeeper 集群创建 Replicated 数据库：

```sql
CREATE DATABASE database_name ENGINE = Replicated('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'shard_name', 'replica_name')
```

<div id="specifics-and-recommendations">
  ## 具体细节与建议
</div>

`Replicated` 数据库中的 DDL 查询与 [ON CLUSTER](../../sql-reference/distributed-ddl.md) 查询的工作方式类似，但也有一些细微差异。

首先，DDL 请求会尝试在发起节点 (最初从用户处接收该请求的主机) 上执行。如果请求未能完成，用户会立即收到错误，其他主机不会继续尝试执行该请求。如果请求已在发起节点上成功完成，则所有其他主机都会自动重试，直到执行完成。发起节点会尝试等待其他主机完成该查询 (等待时间不超过 [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) ，并返回一张表，其中包含该查询在每台主机上的执行状态。

出错时的行为由 [distributed&#95;ddl&#95;output&#95;mode](../../operations/settings/settings.md#distributed_ddl_output_mode) 设置控制。对于 `Replicated` 数据库，建议将其设置为 `null_status_on_timeout`——也就是说，如果某些主机未能在 [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout) 时间内执行完该请求，则不要抛出异常，而是在表中将这些主机的状态显示为 `NULL`。

[system.clusters](../../operations/system-tables/clusters.md) 系统表中包含一个与该 Replicated 数据库同名的集群，它由该数据库的所有副本组成。这个集群会在创建/删除副本时自动更新，也可用于 [Distributed](/zh/engines/table-engines/special/distributed) 表。

创建数据库的新副本时，该副本会自行创建表。如果该副本长时间不可用，并且已经落后于复制日志，它会将本地元数据与 ZooKeeper 中的当前元数据进行比对，把带有数据的多余表移动到单独的非复制表数据库中 (以免误删多余内容) ，创建缺失的表，并在表被重命名时更新表名。数据复制是在 `ReplicatedMergeTree` 层级进行的，也就是说，如果该表不是 replicated，数据就不会被复制 (数据库只负责元数据) 。

允许使用 [`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) 查询，但这些查询不会被复制。数据库引擎只会在当前副本上添加/拉取/移除分区/分片。不过，如果表本身使用的是 Replicated 表引擎，那么在使用 `ATTACH` 之后，数据仍会被复制。

如果你只需要配置集群，而不需要维护表复制，请参阅 [Cluster Discovery](../../operations/cluster-discovery.md) 功能。

<div id="usage-example">
  ## 用法示例
</div>

创建一个包含三个主机的集群：

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

在集群中使用隐式参数创建数据库：

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

执行 DDL 查询：

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

显示系统表：

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

创建分布式表并插入数据：

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

在另一台主机上添加副本：

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

如果在 `zoo_path` 中使用了宏 `{uuid}`，则可以在另一台主机上添加副本：

```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

集群配置如下：

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

分布式表也会从新主机获取数据：

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

<div id="settings">
  ## 设置
</div>

支持以下设置：

| Setting                                                                      | Default                        | Description                                                                                                                                       |
| ---------------------------------------------------------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_broken_tables_ratio`                                                    | 1                              | 如果过期表占全部表的比例超过该值，则不自动恢复副本                                                                                                                         |
| `max_replication_lag_to_enqueue`                                             | 50                             | 如果副本的复制延迟超过该值，则在尝试执行查询时会抛出异常                                                                                                                      |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | 如果超时时间已到，但发起方主机尚未执行该查询，副本将尝试取消查询                                                                                                                  |
| `collection_name`                                                            |                                | server 配置中定义的 collection 名称，其中包含集群身份验证所需的全部信息                                                                                              |
| `check_consistency`                                                          | true                           | 检查本地元数据与 Keeper 中元数据的一致性；如不一致，则执行副本恢复                                                                                                             |
| `max_retries_before_automatic_recovery`                                      | 10                             | 在将副本标记为丢失并通过 snapshot 恢复之前，执行队列条目的最大重试次数 (0 表示无限次)                                                                                                |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | 如果启用，则在 Replicated 数据库中处理 DDLs 时，会在可能的情况下跳过可刷新materialized view 的 temporary tables 的 DDL 创建与交换                                                    |
| `logs_to_keep`                                                               | 1000                           | Replicated database 在 ZooKeeper 中默认保留的日志数量。                                                                                                       |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | 数据库在 ZooKeeper 中的路径。创建数据库时如果省略参数，则使用该值。                                                                                                           |
| `default_replica_shard_name`                                                 | `{shard}`                      | 数据库中副本的 shard 名称。创建数据库时如果省略参数，则使用该值。                                                                                                              |
| `default_replica_name`                                                       | `{replica}`                    | 数据库中副本的名称。创建数据库时如果省略参数，则使用该值。                                                                                                                     |
| `internal_replication`                                                       | false                          | 使用此 Replicated database 的集群创建的 Distributed 表，决定是将数据发送到某一个副本 (内部复制表示集群中的副本会自行完成复制) ，还是发送到所有副本 (无内部复制表示 Distributed 表会将插入的数据发送到全部副本)  |

默认值可以在配置文件中覆盖。

```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
        <internal_replication>false</internal_replication>
    </database_replicated>
</clickhouse>
```