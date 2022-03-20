---
toc_priority: 33
toc_title: 分布式引擎
---

# 分布式引擎 {#distributed}

**分布式引擎本身不存储数据**, 但可以在多个服务器上进行分布式查询。
读是自动并行的。读取时，远程服务器表的索引（如果有的话）会被使用。

## 创建数据表 {#distributed-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]])
[SETTINGS name=value, ...]
```

## 已有数据表 {#distributed-from-a-table}
当 `Distributed` 表指向当前服务器上的一个表时，你可以采用以下语句:


``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster] AS [db2.]name2 ENGINE = Distributed(cluster, database, table[, sharding_key[, policy_name]]) [SETTINGS name=value, ...]
```

**分布式引擎参数**

-   `cluster` - 服务为配置中的集群名

-   `database` - 远程数据库名

-   `table` - 远程数据表名

-   `sharding_key` - (可选) 分片key

-   `policy_name` - (可选) 规则名，它会被用作存储临时文件以便异步发送数据

**详见**

 - [insert_distributed_sync](../../../operations/settings/settings.md#insert_distributed_sync) 设置
 - [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) 查看示例
 
 **分布式设置**

- `fsync_after_insert` - 对异步插入到分布式的文件数据执行`fsync`。确保操作系统将所有插入的数据刷新到启动节点**磁盘上的一个文件**中。

- `fsync_directories` - 对目录执行`fsync`。保证操作系统在分布式表上进行异步插入相关操作(插入后，发送数据到分片等)后刷新目录元数据.

- `bytes_to_throw_insert` - 如果超过这个数量的压缩字节将等待异步INSERT，将抛出一个异常。0 - 不抛出。默认值0.

- `bytes_to_delay_insert` - 如果超过这个数量的压缩字节将等待异步INSERT，查询将被延迟。0 - 不要延迟。默认值0.

- `max_delay_to_insert` - 最大延迟多少秒插入数据到分布式表，如果有很多挂起字节异步发送。默认值60。

- `monitor_batch_inserts` - 等同于 [distributed_directory_monitor_batch_inserts](../../../operations/settings/settings.md#distributed_directory_monitor_batch_inserts)

- `monitor_split_batch_on_failure` - 等同于[distributed_directory_monitor_split_batch_on_failure](../../../operations/settings/settings.md#distributed_directory_monitor_split_batch_on_failure)

- `monitor_sleep_time_ms` - 等同于 [distributed_directory_monitor_sleep_time_ms](../../../operations/settings/settings.md#distributed_directory_monitor_sleep_time_ms)

- `monitor_max_sleep_time_ms` - 等同于 [distributed_directory_monitor_max_sleep_time_ms](../../../operations/settings/settings.md#distributed_directory_monitor_max_sleep_time_ms)

!!! note "备注"

    **稳定性设置** (`fsync_...`):

    - 只影响异步插入(例如:`insert_distributed_sync=false`), 当数据首先存储在启动节点磁盘上，然后再异步发送到shard。
    — 可能会显著降低`insert`的性能
    - 影响将存储在分布式表文件夹中的数据写入 **接受您插入的节点** 。如果你需要保证写入数据到底层的MergeTree表中，请参阅 `system.merge_tree_settings` 中的持久性设置(`...fsync...`)

    **插入限制设置** (`..._insert`) 请见:

    - [insert_distributed_sync](../../../operations/settings/settings.md#insert_distributed_sync) 设置
    - [prefer_localhost_replica](../../../operations/settings/settings.md#settings-prefer-localhost-replica) 设置
    - `bytes_to_throw_insert` 在 `bytes_to_delay_insert` 之前处理，所以你不应该设置它的值小于 `bytes_to_delay_insert`
**示例**

``` sql
CREATE TABLE hits_all AS hits
ENGINE = Distributed(logs, default, hits[, sharding_key[, policy_name]])
SETTINGS
    fsync_after_insert=0,
    fsync_directories=0;
```

数据将从`logs`集群中的所有服务器中，从位于集群中的每个服务器上的`default.hits`表读取。。
数据不仅在远程服务器上读取，而且在远程服务器上进行部分处理(在可能的范围内)。
例如，对于带有 `GROUP BY`的查询，数据将在远程服务器上聚合，聚合函数的中间状态将被发送到请求者服务器。然后将进一步聚合数据。

您可以使用一个返回字符串的常量表达式来代替数据库名称。例如: `currentDatabase()`。

## 集群 {#distributed-clusters}


集群是通过[服务器配置文件](../../../operations/configuration-files.md)来配置的

``` xml
<remote_servers>
    <logs>
        <!-- 分布式查询的服务器间集群密码
             默认值:无密码(将不执行身份验证)

             如果设置了，那么分布式查询将在分片上验证，所以至少:
             - 这样的集群应该存在于shard上
             - 这样的集群应该有相同的密码。

             而且(这是更重要的)，initial_user将作为查询的当前用户使用。
        -->
        <!-- <secret></secret> -->
        <shard>
            <!-- 可选的。写数据时分片权重。 默认: 1. -->
            <weight>1</weight>
            <!-- 可选的。是否只将数据写入其中一个副本。默认值:false(将数据写入所有副本)。 -->
            <internal_replication>false</internal_replication>
            <replica>
                <!-- 可选的。负载均衡副本的优先级，请参见（load_balancing 设置)。默认值:1(值越小优先级越高)。 -->
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

这里定义了一个名为’logs’的集群，它由两个分片组成，每个分片包含两个副本。
分片是指包含数据不同部分的服务器（要读取所有数据，必须访问所有分片）。
副本是存储复制数据的服务器（要读取所有数据，访问任一副本上的数据即可）。

集群名称不能包含点号。

每个服务器需要指定 `host`，`port`，和可选的 `user`，`password`，`secure`，`compression` 的参数：

- `host` – 远程服务器地址。可以域名、IPv4或IPv6。如果指定域名，则服务在启动时发起一个 DNS 请求，并且请求结果会在服务器运行期间一直被记录。如果 DNS 请求失败，则服务不会启动。如果你修改了 DNS 记录，则需要重启服务。
- `port` – 消息传递的 TCP 端口（「tcp_port」配置通常设为 9000）。不要跟 http_port 混淆。
- `user` – 用于连接远程服务器的用户名。默认值：default。该用户必须有权限访问该远程服务器。访问权限配置在 users.xml 文件中。更多信息，请查看«访问权限»部分。
- `password` – 用于连接远程服务器的密码。默认值：空字符串。
- `secure` – 是否使用ssl进行连接，设为true时，通常也应该设置 `port` = 9440。服务器也要监听 `<tcp_port_secure>9440</tcp_port_secure>` 并有正确的证书。
- `compression` - 是否使用数据压缩。默认值：true。

配置了副本，读取操作会从每个分片里选择一个可用的副本。可配置负载平衡算法（挑选副本的方式） - 请参阅«load_balancing»设置。
如果跟服务器的连接不可用，则在尝试短超时的重连。如果重连失败，则选择下一个副本，依此类推。如果跟所有副本的连接尝试都失败，则尝试用相同的方式再重复几次。
该机制有利于系统可用性，但不保证完全容错：如有远程服务器能够接受连接，但无法正常工作或状况不佳。

你可以配置一个（这种情况下，查询操作更应该称为远程查询，而不是分布式查询）或任意多个分片。在每个分片中，可以配置一个或任意多个副本。不同分片可配置不同数量的副本。

可以在配置中配置任意数量的集群。

要查看集群，可使用«system.clusters»表。

通过分布式引擎可以像使用本地服务器一样使用集群。但是，集群不是自动扩展的：你必须编写集群配置到服务器配置文件中（最好，给所有集群的服务器写上完整配置）。

不支持用分布式表查询别的分布式表（除非该表只有一个分片）。或者说，要用分布表查查询«最终»的数据表。
分布式引擎需要将集群信息写入配置文件。配置文件中的集群信息会即时更新，无需重启服务器。如果你每次是要向不确定的一组分片和副本发送查询，则不适合创建分布式表 - 而应该使用«远程»表函数。 请参阅«表函数»部分。

## 写入数据

向集群写数据的方法有两种：

一，自已指定要将哪些数据写入哪些服务器，并直接在每个分片上执行写入。换句话说，在分布式表上«查询»，在数据表上 INSERT。
这是最灵活的解决方案 – 你可以使用任何分片方案，对于复杂业务特性的需求，这可能是非常重要的。
这也是最佳解决方案，因为数据可以完全独立地写入不同的分片。

二，在分布式表上执行 INSERT。在这种情况下，分布式表会跨服务器分发插入数据。
为了写入分布式表，必须要配置分片键（最后一个参数）。当然，如果只有一个分片，则写操作在没有分片键的情况下也能工作，因为这种情况下分片键没有意义。

每个分片都可以在配置文件中定义权重。默认情况下，权重等于1。数据依据分片权重按比例分发到分片上。例如，如果有两个分片，第一个分片的权重是9，而第二个分片的权重是10，则发送 9 / 19 的行到第一个分片， 10 / 19 的行到第二个分片。

分片可在配置文件中定义 ‘internal_replication’ 参数。

此参数设置为«true»时，写操作只选一个正常的副本写入数据。如果分布式表的子表是复制表(\*ReplicaMergeTree)，请使用此方案。换句话说，这其实是把数据的复制工作交给实际需要写入数据的表本身而不是分布式表。

若此参数设置为«false»（默认值），写操作会将数据写入所有副本。实质上，这意味着要分布式表本身来复制数据。这种方式不如使用复制表的好，因为不会检查副本的一致性，并且随着时间的推移，副本数据可能会有些不一样。

选择将一行数据发送到哪个分片的方法是，首先计算分片表达式，然后将这个计算结果除以所有分片的权重总和得到余数。该行会发送到那个包含该余数的从’prev_weight’到’prev_weights + weight’的半闭半开区间对应的分片上，其中 ‘prev_weights’ 是该分片前面的所有分片的权重和，‘weight’ 是该分片的权重。例如，如果有两个分片，第一个分片权重为9，而第二个分片权重为10，则余数在 \[0,9) 中的行发给第一个分片，余数在 \[9,19) 中的行发给第二个分片。

分片表达式可以是由常量和表列组成的任何返回整数表达式。例如，您可以使用表达式 ‘rand()’ 来随机分配数据，或者使用 ‘UserID’ 来按用户 ID 的余数分布（相同用户的数据将分配到单个分片上，这可降低带有用户信息的 IN 和 JOIN 的语句运行的复杂度）。如果该列数据分布不够均匀，可以将其包装在散列函数中：intHash64(UserID)。

这种简单的用余数来选择分片的方案是有局限的，并不总适用。它适用于中型和大型数据（数十台服务器）的场景，但不适用于巨量数据（数百台或更多服务器）的场景。后一种情况下，应根据业务特性需求考虑的分片方案，而不是直接用分布式表的多分片。

SELECT 查询会被发送到所有分片，并且无论数据在分片中如何分布（即使数据完全随机分布）都可正常工作。添加新分片时，不必将旧数据传输到该分片。你可以给新分片分配大权重然后写新数据 - 数据可能会稍分布不均，但查询会正确高效地运行。

下面的情况，你需要关注分片方案：

-   使用需要特定键连接数据（ IN 或 JOIN ）的查询。如果数据是用该键进行分片，则应使用本地 IN 或 JOIN 而不是 GLOBAL IN 或 GLOBAL JOIN，这样效率更高。
-   使用大量服务器（上百或更多），但有大量小查询（个别客户的查询 - 网站，广告商或合作伙伴）。为了使小查询不影响整个集群，让单个客户的数据处于单个分片上是有意义的。或者 你可以配置两级分片：将整个集群划分为«层»，一个层可以包含多个分片。单个客户的数据位于单个层上，根据需要将分片添加到层中，层中的数据随机分布。然后给每层创建分布式表，再创建一个全局的分布式表用于全局的查询。

数据是异步写入的。对于分布式表的 INSERT，数据块只写本地文件系统。之后会尽快地在后台发送到远程服务器。发送数据的周期性是由[distributed_directory_monitor_sleep_time_ms](../../../operations/settings/settings.md#distributed_directory_monitor_sleep_time_ms)和[distributed_directory_monitor_max_sleep_time_ms](../../../operations/settings/settings.md#distributed_directory_monitor_max_sleep_time_ms)设置。分布式引擎会分别发送每个插入数据的文件，但是你可以使用[distributed_directory_monitor_batch_inserts](../../../operations/settings/settings.md#distributed_directory_monitor_batch_inserts)设置启用批量发送文件。该设置通过更好地利用本地服务器和网络资源来提高集群性能。你应该检查表目录`/var/lib/clickhouse/data/database/table/`中的文件列表(等待发送的数据)来检查数据是否发送成功。执行后台任务的线程数可以通过[background_distributed_schedule_pool_size](../../../operations/settings/settings.md#background_distributed_schedule_pool_size)设置。

如果在 INSERT 到分布式表时服务器节点丢失或重启（如，设备故障），则插入的数据可能会丢失。如果在表目录中检测到损坏的数据分片，则会将其转移到«broken»子目录，并不再使用。


## 读取数据 {#distributed-reading-data}
 
当查询一个`Distributed`表时，`SELECT`查询被发送到所有的分片，不管数据是如何分布在分片上的(它们可以完全随机分布)。当您添加一个新分片时，您不必将旧数据传输到它。相反，您可以使用更重的权重向其写入新数据——数据的分布会稍微不均匀，但查询将正确有效地工作。

当启用`max_parallel_replicas`选项时，查询处理将在单个分片中的所有副本之间并行化。更多信息，请参见[max_parallel_replicas](../../../operations/settings/settings.md#settings-max_parallel_replicas)。

要了解更多关于分布式`in`和`global in`查询是如何处理的，请参考[这里](../../../sql-reference/operators/in.md#select-distributed-subqueries)文档。

## 虚拟列 {#virtual-columns}

-   `_shard_num` — 表`system.clusters` 中的  `shard_num` 值 . 数据类型: [UInt32](../../../sql-reference/data-types/int-uint.md).

!!! note "备注"
    因为 [remote](../../../sql-reference/table-functions/remote.md) 和 [cluster](../../../sql-reference/table-functions/cluster.md) 表方法内部创建了分布式表， `_shard_num` 对他们都有效.

**详见**
-   [虚拟列](../../../engines/table-engines/index.md#table_engines-virtual_columns) 描述
-   [background_distributed_schedule_pool_size](../../../operations/settings/settings.md#background_distributed_schedule_pool_size) 设置
-   [shardNum()](../../../sql-reference/functions/other-functions.md#shard-num) 和 [shardCount()](../../../sql-reference/functions/other-functions.md#shard-count) 方法


[原始文章](https://clickhouse.com/docs/en/operations/table_engines/distributed/) <!--hide-->
