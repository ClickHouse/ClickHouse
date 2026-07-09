---
description: '使用 Distributed 引擎的表本身不存储任何数据，而是支持在多台服务器上进行分布式查询处理。读取会自动并行化。读取时，如果远程服务器上存在表索引，则会使用这些索引。'
sidebar_label: 'Distributed'
sidebar_position: 10
slug: /engines/table-engines/special/distributed
title: 'Distributed 表引擎'
doc_type: 'reference'
---

:::warning Cloud 中的 Distributed 引擎
要在 ClickHouse Cloud 中创建分布式表引擎，可以使用 [`remote` 和 `remoteSecure`](../../../sql-reference/table-functions/remote) 表函数。
在 ClickHouse Cloud 中不能使用 `Distributed(...)` 语法。
:::

使用 Distributed 引擎的表本身不存储任何数据，而是支持在多台服务器上进行分布式查询处理。
读取会自动并行化。读取时，如果远程服务器上存在表索引，则会使用这些索引。

<div id="distributed-creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]])
[SETTINGS name=value, ...]
```

<div id="distributed-from-a-table">
  ### 从表继承
</div>

当 `Distributed` 表指向当前服务器上的某个表时，你可以采用该表的 schema：

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster] AS [db2.]name2 ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]]) [SETTINGS name=value, ...]
```

<div id="distributed-parameters">
  ### Distributed 参数
</div>

| 参数                  | 说明                                                                                                                                                                                                                                                               |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`           | 服务器配置文件中的集群名称                                                                                                                                                                                                                                                    |
| `database`          | 远程数据库的名称                                                                                                                                                                                                                                                         |
| `table`             | 远程表的名称                                                                                                                                                                                                                                                           |
| `sharding_key` (可选) | 分片键。<br /> 在以下情况下，必须指定 `sharding_key`：<ul><li>向分布式表执行 `INSERT` 时 (因为表引擎需要 `sharding_key` 来确定如何拆分数据) 。不过，如果启用了 `insert_distributed_one_random_shard` 设置，则 `INSERT` 不需要分片键。</li><li>与 `optimize_skip_unused_shards` 一起使用时，因为需要 `sharding_key` 来确定应查询哪些分片</li></ul> |
| `policy_name` (可选)  | 策略名称，将用于存储后台发送时产生的临时 File                                                                                                                                                                                                                                        |

**另请参见**

* [distributed&#95;foreground&#95;insert](../../../operations/settings/settings.md#distributed_foreground_insert) 设置
* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) 中的示例

<div id="distributed-settings">
  ### Distributed 设置
</div>

| Setting                                    | 描述                                                                                                                                                                                  | 默认值                            |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `fsync_after_insert`                       | 在向 Distributed 执行后台插入后，对文件数据执行 `fsync`。这可确保操作系统将全部已插入的数据刷新到**发起节点**磁盘上的文件中。                                                                                                         | `false`                        |
| `fsync_directories`                        | 对目录执行 `fsync`。这可确保在与向 Distributed 表执行后台插入相关的操作 (例如插入后、将数据发送到分片后等) 之后，操作系统刷新目录元数据。                                                                                                   | `false`                        |
| `skip_unavailable_shards`                  | 如果为 true，ClickHouse 会静默跳过不可用的分片。此设置的行为由 `skip_unavailable_shards_mode` 参数控制。                                                                                                        | `false`                        |
| `skip_unavailable_shards_mode`             | 控制在启用 `skip_unavailable_shards` 时，会忽略来自远程分片的哪些异常：`unavailable` 仅忽略连接错误；`unavailable_or_table_missing` 还会忽略缺失的表或数据库；`unavailable_or_exception_before_processing` 还会忽略分片返回数据前收到的任何异常。 | `unavailable_or_table_missing` |
| `bytes_to_throw_insert`                    | 如果后台 `INSERT` 的待处理压缩字节数超过该值，则会抛出异常。`0` - 不抛出。                                                                                                                                       | `0`                            |
| `bytes_to_delay_insert`                    | 如果后台 `INSERT` 的待处理压缩字节数超过该值，则会延迟查询。`0` - 不延迟。                                                                                                                                       | `0`                            |
| `max_delay_to_insert`                      | 当后台发送存在大量待处理字节时，向 Distributed 表插入数据的最大延迟时间 (秒) 。                                                                                                                                    | `60`                           |
| `background_insert_batch`                  | 与 [`distributed_background_insert_batch`](../../../operations/settings/settings.md#distributed_background_insert_batch) 相同                                                          | `0`                            |
| `background_insert_split_batch_on_failure` | 与 [`distributed_background_insert_split_batch_on_failure`](../../../operations/settings/settings.md#distributed_background_insert_split_batch_on_failure) 相同                        | `0`                            |
| `background_insert_sleep_time_ms`          | 与 [`distributed_background_insert_sleep_time_ms`](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) 相同                                          | `0`                            |
| `background_insert_max_sleep_time_ms`      | 与 [`distributed_background_insert_max_sleep_time_ms`](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms) 相同                                  | `0`                            |
| `flush_on_detach`                          | 在 `DETACH`/`DROP`/服务器关闭时，将数据刷新到远程节点。                                                                                                                                                | `true`                         |

:::note
**持久性设置** (`fsync_...`) ：

* 仅影响后台 `INSERT` (即 `distributed_foreground_insert=false`) ：数据首先存储在发起节点磁盘上，随后再由后台发送到各个分片。
* 可能会显著降低 `INSERT` 性能
* 影响的是将存储在分布式表文件夹中的数据写入**接收插入请求的节点**。如果你需要保证数据写入到底层 MergeTree 表，请参见 `system.merge_tree_settings` 中的持久性设置 (`...fsync...`)

对于**插入限制设置** (`..._insert`) ，另请参见：

* [`distributed_foreground_insert`](../../../operations/settings/settings.md#distributed_foreground_insert) 设置
* [`prefer_localhost_replica`](/zh/operations/settings/settings#prefer_localhost_replica) 设置
* `bytes_to_throw_insert` 会先于 `bytes_to_delay_insert` 处理，因此不应将其设置为小于 `bytes_to_delay_insert` 的值
  :::

**示例**

```sql
CREATE TABLE hits_all AS hits
ENGINE = Distributed(logs, default, hits[, sharding_key[, policy_name]])
SETTINGS
    fsync_after_insert=0,
    fsync_directories=0;
```

数据将从 `logs` 集群中的所有服务器读取，来源是集群中每台服务器上的 `default.hits` 表。数据不仅会被读取，还会尽可能在远程服务器上进行部分处理。例如，对于带有 `GROUP BY` 的查询，数据会先在远程服务器上聚合，然后将聚合函数的中间状态发送到发起请求的服务器，之后再继续聚合。

你也可以用返回字符串的常量表达式来代替数据库名称。例如：`currentDatabase()`。

<div id="distributed-clusters">
  ## 集群
</div>

可在[服务器配置文件](../../../operations/configuration-files.md)中配置集群：

```xml
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
            <!-- Optional. The shard name.  Must be non-empty and unique among shards in the cluster. If not specified, will be empty. -->
            <name>shard_01</name>
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
            <name>shard_02</name>
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

这里定义了一个名为 `logs` 的集群，它由两个分片组成，每个分片包含两个副本。分片是指包含不同部分数据的服务器 (要读取全部数据，必须访问所有分片) 。副本是数据的复制服务器 (要读取全部数据，只需访问任意一个副本上的数据) 。

集群名称不能包含点号。

需要为每台服务器指定参数 `host`、`port`，以及可选参数 `user`、`password`、`secure`、`compression`、`bind_host`：

| Parameter     | Description                                                                                                                | Default Value |
| ------------- | -------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `host`        | 远程服务器的地址。可以使用域名，也可以使用 IPv4 或 IPv6 地址。如果指定的是域名，服务器会在启动时发起 DNS 请求，并在服务器运行期间一直缓存结果。如果 DNS 请求失败，服务器将无法启动。如果更改了 DNS 记录，请重启服务器。  | -             |
| `port`        | 用于消息通信的 TCP 端口 (即 config 中的 `tcp_port`，通常设置为 9000) 。不要与 `http_port` 混淆。                                                    | -             |
| `user`        | 用于连接远程服务器的用户名。该用户必须具有连接到指定服务器的访问权限。访问配置在 `users.xml` 文件中。更多信息，请参见 [访问权限](../../../guides/sre/user-management/index.md) 部分。 | `default`     |
| `password`    | 用于连接远程服务器的密码 (不做掩码处理) 。                                                                                                    | &#39;&#39;    |
| `secure`      | 是否使用安全的 SSL/TLS 连接。通常还需要指定端口 (默认安全端口为 `9440`) 。服务器应监听 `<tcp_port_secure>9440</tcp_port_secure>`，并配置正确的证书。                  | `false`       |
| `compression` | 使用数据压缩。                                                                                                                    | `true`        |
| `bind_host`   | 此节点连接远程服务器时使用的源地址。仅支持 IPv4 地址。适用于高级部署用例，即需要设置 ClickHouse 分布式查询所使用的源 IP 地址时。                                                | -             |

指定副本时，读取每个分片的数据都会从其可用副本中选择一个。你可以配置负载均衡算法 (即优先访问哪个副本) ——参见 [load&#95;balancing](../../../operations/settings/settings.md#load_balancing) 设置。如果无法与服务器建立连接，则会以较短的超时时间尝试连接。如果连接失败，则会选择下一个副本，依次尝试所有副本。如果所有副本的连接尝试都失败了，就会按相同方式重复尝试多次。这有助于提高弹性，但并不能提供完整的容错能力：远程服务器可能接受连接，但无法正常工作，或性能较差。

你也可以只指定一个分片 (这种情况下，应称为 remote 查询，而非 distributed 查询) ，或者指定任意多个分片。在每个分片中，你可以指定一个或多个副本，数量不限。你还可以为每个分片指定不同数量的副本。

你可以在配置中指定任意数量的集群。

要查看你的集群，请使用 `system.clusters` 表。

`Distributed` 引擎允许像使用本地服务器一样使用集群。不过，集群配置不能动态指定，必须在服务器配置文件中配置。通常，集群中的所有服务器都会使用相同的集群配置 (尽管这不是必需的) 。配置文件中的集群可在不重启服务器的情况下动态更新。

如果你需要每次都将查询发送到一组未知的分片和副本，就无需创建 `Distributed` 表——请改用 `remote` 表函数。参见 [Table functions](../../../sql-reference/table-functions/index.md) 一节。

<div id="distributed-writing-data">
  ## 写入数据
</div>

向集群写入数据有两种方法：

首先，你可以指定将哪些数据写入哪些服务器，并直接在每个分片上执行写入。换句话说，直接对集群中 `Distributed` 表所指向的远程表执行 `INSERT` 语句。这是最灵活的方案，因为你可以使用任何分片方案，甚至可以根据特定业务场景的要求采用较为复杂的分片方案。这也是最优的方案，因为数据可以完全独立地写入不同的分片。

第二，你可以对 `Distributed` 表执行 `INSERT` 语句。在这种情况下，表会自行将插入的数据分发到各个服务器。要写入 `Distributed` 表，必须配置 `sharding_key` 参数 (只有一个分片时除外) 。

每个分片都可以在配置文件中定义一个 `<weight>`。默认情况下，权重为 `1`。数据会按照与分片权重成比例的数量分布到各个分片。系统会先将所有分片的权重求和，然后用每个分片的权重除以总权重，以确定该分片所占的比例。例如，如果有两个分片，第一个权重为 1，第二个权重为 2，那么插入的行中，三分之一 (1 / 3) 会发送到第一个分片，三分之二 (2 / 3) 会发送到第二个分片。

每个分片都可以在配置文件中定义 `internal_replication` 参数。如果该参数设置为 `true`，写操作会选择第一个健康的副本并向其写入数据。如果 `Distributed` 表底层的表是复制表 (例如任意 `Replicated*MergeTree` 表引擎) ，请使用此设置。某个表副本会接收写入，随后数据会自动复制到其他副本。

如果 `internal_replication` 设置为 `false` (默认值) ，数据会写入所有副本。在这种情况下，`Distributed` 表会自行复制数据。这比使用复制表更差，因为不会检查副本的一致性，随着时间推移，它们会包含略有差异的数据。

要选择某一行数据应发送到哪个分片，系统会分析分片表达式，并将其除以所有分片总权重后取余。该行会被发送到与余数所在半区间对应的分片，这个半区间范围是从 `prev_weights` 到 `prev_weights + weight`，其中 `prev_weights` 是编号更小的分片的总权重，`weight` 是当前分片的权重。例如，如果有两个分片，第一个权重为 9，第二个权重为 10，那么余数落在区间 [0, 9) 时，该行会发送到第一个分片；余数落在区间 [9, 19) 时，则发送到第二个分片。

分片表达式可以是由常量和表列组成的任意表达式，只要其返回整数即可。例如，你可以使用表达式 `rand()` 来随机分布数据，或者使用 `UserID` 按用户 ID 取余进行分布 (这样单个用户的数据会位于同一个分片上，从而简化按用户执行 `IN` 和 `JOIN`) 。如果某一列的分布不够均匀，可以将其包装在哈希函数中，例如 `intHash64(UserID)`。

简单的除法取余是一种有局限性的分片方案，并不总是合适。它适用于中大型数据量 (数十台服务器) ，但不适用于超大规模数据量 (数百台服务器或更多) 。对于后一种情况，应采用业务场景所需的分片方案，而不是依赖 `Distributed` 表中的条目。

在以下情况下，你应该关注分片方案：

* 使用了需要按特定键连接数据的查询 (`IN` 或 `JOIN`) 。如果数据按该键分片，你就可以使用本地 `IN` 或 `JOIN`，而不是 `GLOBAL IN` 或 `GLOBAL JOIN`，这样效率会高得多。
* 使用了大量服务器 (数百台或更多) ，并且伴随大量小查询，例如查询单个客户的数据 (如网站、广告商或合作伙伴) 。为了让这些小查询不会影响整个集群，将单个客户的数据放在单个分片上是有意义的。另一种方式是设置双层分片：将整个集群划分为多个“层”，每一层可以由多个分片组成。单个客户的数据位于单个层中，但可以根据需要向该层添加分片，且数据会在这些分片内部随机分布。为每一层创建 `Distributed` 表，再创建一个共享的分布式表用于全局查询。

数据在后台写入。插入到表中时，数据块只会写入本地文件系统。系统随后会尽快在后台将数据发送到远程服务器。发送数据的周期由 [distributed&#95;background&#95;insert&#95;sleep&#95;time&#95;ms](../../../operations/settings/settings.md#distributed_background_insert_sleep_time_ms) 和 [distributed&#95;background&#95;insert&#95;max&#95;sleep&#95;time&#95;ms](../../../operations/settings/settings.md#distributed_background_insert_max_sleep_time_ms) 设置控制。`Distributed` 引擎会分别发送每个包含插入数据的文件，但你也可以通过 [distributed&#95;background&#95;insert&#95;batch](../../../operations/settings/settings.md#distributed_background_insert_batch) 设置启用文件批次发送。此设置可通过更充分地利用本地服务器和网络资源来提升集群性能。你应检查表目录中的文件列表 (等待发送的数据) ，以确认数据是否已成功发送：`/var/lib/clickhouse/data/database/table/`。执行后台任务的线程数可以通过 [background&#95;distributed&#95;schedule&#95;pool&#95;size](/zh/operations/server-configuration-parameters/settings#background_distributed_schedule_pool_size) 设置。

如果在向 `Distributed` 表执行 `INSERT` 后，服务器宕机或发生异常重启 (例如由于硬件故障) ，则已插入的数据可能会丢失。如果在表目录中检测到损坏的数据分区片段，它会被转移到 `broken` 子目录中，并且不再使用。

<div id="distributed-reading-data">
  ## 读取数据
</div>

查询 `Distributed` 表时，`SELECT` 查询会发送到所有分片，因此无论数据在各分片之间如何分布 (甚至可以是完全随机分布) ，查询都能正常执行。添加新分片时，无需将旧数据迁移到该分片。相反，你可以通过赋予更高的权重将新数据写入其中——这样数据分布会略有不均，但查询仍能正确高效地运行。

启用 `max_parallel_replicas` 选项后，查询处理会在单个分片内的所有副本上并行执行。更多信息，请参见 [max&#95;parallel&#95;replicas](../../../operations/settings/settings.md#max_parallel_replicas) 部分。

如需进一步了解分布式 `in` 和 `global in` 查询的处理方式，请参阅[此文档](/zh/sql-reference/operators/in#distributed-subqueries)。

<div id="virtual-columns">
  ## 虚拟列
</div>

<div id="_shard_num">
  #### _Shard_num
</div>

`_shard_num` — 包含表 `system.clusters` 中的 `shard_num` 值。类型：[UInt32](../../../sql-reference/data-types/int-uint.md)。

:::note
由于 [`remote`](../../../sql-reference/table-functions/remote.md) 和 [`cluster](../../../sql-reference/table-functions/cluster.md) 表函数会在内部创建临时 Distributed 表，因此 `&#95;shard&#95;num&#96; 在这里同样可用。
:::

**另请参见**

* [虚拟列](../../../engines/table-engines/index.md#table_engines-virtual_columns)说明
* [`background_distributed_schedule_pool_size`](/zh/operations/server-configuration-parameters/settings#background_distributed_schedule_pool_size) 设置
* [`shardNum()`](../../../sql-reference/functions/other-functions.md#shardNum) 和 [`shardCount()`](../../../sql-reference/functions/other-functions.md#shardCount) 函数