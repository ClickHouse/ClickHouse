# [experimental] Replicated {#replicated}

该引擎基于[Atomic](../../engines/database-engines/atomic.md)引擎。它支持通过将DDL日志写入ZooKeeper并在给定数据库的所有副本上执行的元数据复制。

一个ClickHouse服务器可以同时运行和更新多个复制的数据库。但是同一个复制的数据库不能有多个副本。

## 创建数据库 {#creating-a-database}

``` sql
CREATE DATABASE testdb ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**引擎参数**

-   `zoo_path` — ZooKeeper地址，同一个ZooKeeper路径对应同一个数据库。
-   `shard_name` — 分片的名字。数据库副本按`shard_name`分组到分片中。
-   `replica_name` — 副本的名字。同一分片的所有副本的副本名称必须不同。

!!! note "警告"
对于[ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication)表，如果没有提供参数，则使用默认参数:`/clickhouse/tables/{uuid}/{shard}`和`{replica}`。这些可以在服务器设置[default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path)和[default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name)中更改。宏`{uuid}`被展开到表的uuid， `{shard}`和`{replica}`被展开到服务器配置的值，而不是数据库引擎参数。但是在将来，可以使用Replicated数据库的`shard_name`和`replica_name`。

## 使用方式 {#specifics-and-recommendations}

使用`Replicated`数据库的DDL查询的工作方式类似于[ON CLUSTER](../../sql-reference/distributed-ddl.md)查询，但有细微差异。

首先，DDL请求尝试在启动器(最初从用户接收请求的主机)上执行。如果请求没有完成，那么用户立即收到一个错误，其他主机不会尝试完成它。如果在启动器上成功地完成了请求，那么所有其他主机将自动重试，直到完成请求。启动器将尝试在其他主机上等待查询完成(不超过[distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout))，并返回一个包含每个主机上查询执行状态的表。

错误情况下的行为是由[distributed_ddl_output_mode](../../operations/settings/settings.md#distributed_ddl_output_mode)设置调节的，对于`Replicated`数据库，最好将其设置为`null_status_on_timeout` - 例如，如果一些主机没有时间执行[distributed_ddl_task_timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)的请求，那么不要抛出异常，但在表中显示它们的`NULL`状态。

[system.clusters](../../operations/system-tables/clusters.md)系统表包含一个名为复制数据库的集群，它包含数据库的所有副本。当创建/删除副本时，这个集群会自动更新，它可以用于[Distributed](../../engines/table-engines/special/distributed.md#distributed)表。

当创建数据库的新副本时，该副本会自己创建表。如果副本已经不可用很长一段时间，并且已经滞后于复制日志-它用ZooKeeper中的当前元数据检查它的本地元数据，将带有数据的额外表移动到一个单独的非复制数据库(以免意外地删除任何多余的东西)，创建缺失的表，如果表名已经被重命名，则更新表名。数据在`ReplicatedMergeTree`级别被复制，也就是说，如果表没有被复制，数据将不会被复制(数据库只负责元数据)。

允许[`ALTER TABLE ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md)查询，但不允许复制。数据库引擎将只向当前副本添加/获取/删除分区/部件。但是，如果表本身使用了Replicated表引擎，那么数据将在使用`ATTACH`后被复制。
## 使用示例 {#usage-example}

创建三台主机的集群:

``` sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

运行DDL:

``` sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

``` text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐ 
│ shard1|replica1      │    0    │       │          2          │        0         │ 
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

显示系统表:

``` sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local 
FROM system.clusters WHERE cluster='r';
```

``` text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐ 
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │ 
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

创建分布式表并插入数据:

``` sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

``` text
┌─hosts─┬─groupArray(n)─┐ 
│ node1 │  [1,3,5,7,9]  │   
│ node2 │  [0,2,4,6,8]  │    
└───────┴───────────────┘
```

向一台主机添加副本:

``` sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

集群配置如下所示:

``` text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐ 
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │ 
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

分布式表也将从新主机获取数据:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐ 
│ node2 │  [1,3,5,7,9]  │   
│ node4 │  [0,2,4,6,8]  │    
└───────┴───────────────┘
```
