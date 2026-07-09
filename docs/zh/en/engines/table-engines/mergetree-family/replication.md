---
description: 'ClickHouse 中 Replicated* 系列表引擎的数据复制概述'
sidebar_label: 'Replicated*'
sidebar_position: 20
slug: /engines/table-engines/mergetree-family/replication
title: 'Replicated* 系列表引擎'
doc_type: 'reference'
---

:::note
在 ClickHouse Cloud 中，复制由系统代为管理。创建表时请不要添加参数。例如，在下文中，您需要将：

```sql
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/table_name',
    '{replica}'
)
```

使用：

```sql
ENGINE = ReplicatedMergeTree
```

:::

只有 MergeTree 家族中的表支持复制

* ReplicatedSummingMergeTree
* ReplicatedCoalescingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedGraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree

复制是在单个表级别进行的，而不是在整个服务器级别。一个服务器可以同时存储复制表和非复制表。

复制不依赖于分片。每个分片都有各自独立的复制。

`INSERT` 和 `ALTER` 查询的压缩数据会被复制 (更多信息请参见 [ALTER](/zh/sql-reference/statements/alter) 文档) 。

`CREATE`、`DROP`、`ATTACH`、`DETACH` 和 `RENAME` 查询只在单台服务器上执行，不会被复制：

* `CREATE TABLE` 查询会在执行该查询的服务器上创建一个新的可复制表。如果该表已存在于其他服务器上，则会新增一个副本。
* `DROP TABLE` 查询会删除执行该查询的服务器上的副本。
* `RENAME` 查询会重命名某个副本上的表。换句话说，复制表在不同副本上可以有不同的名称。

ClickHouse 使用 [ClickHouse Keeper](/zh/guides/sre/keeper/index.md) 存储副本元信息。也可以使用 3.4.5 或更高版本的 ZooKeeper，但推荐使用 ClickHouse Keeper。

要使用复制，请在 [zookeeper](/zh/operations/server-configuration-parameters/settings#zookeeper) 服务器配置部分中设置参数。

:::note
不要忽视安全设置。ClickHouse 支持 ZooKeeper 安全子系统的 `digest` [ACL scheme](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)。
:::

设置 ClickHouse Keeper 集群地址的示例：

```xml
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

ClickHouse 还支持将副本的元信息存储在辅助 ZooKeeper 集群中。为此，请将 ZooKeeper 集群名称和路径作为引擎参数传入。
换句话说，它支持将不同表的元数据存储到不同的 ZooKeeper 集群中。

设置辅助 ZooKeeper 集群地址的示例：

```xml
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

要将表元数据存储在辅助 ZooKeeper 集群中，而非默认的 ZooKeeper 集群，我们可以使用 SQL 按如下方式创建使用
ReplicatedMergeTree 引擎的表：

```sql
CREATE TABLE table_name ( ... ) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```

您可以指定任何现有的 ZooKeeper 集群，系统会使用其中的一个目录来存储自身数据 (该目录在创建可复制表时指定) 。

如果未在配置文件中设置 ZooKeeper，则无法创建复制表，且任何现有的复制表都将处于只读状态。

ZooKeeper 不用于 `SELECT` 查询，因为复制不会影响 `SELECT` 的性能，查询速度与非复制表相同。在查询分布式复制表时，ClickHouse 的行为由设置 [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](/zh/operations/settings/settings.md/#max_replica_delay_for_distributed_queries) 和 [fallback&#95;to&#95;stale&#95;replicas&#95;for&#95;distributed&#95;queries](/zh/operations/settings/settings.md/#fallback_to_stale_replicas_for_distributed_queries) 控制。

对于每个 `INSERT` 查询，大约会通过几个事务向 ZooKeeper 添加十个条目。 (更准确地说，这是针对每个插入的数据块；一个 INSERT 查询包含一个块，或者每 `max_insert_block_size = 1048576` 行一个块。) 因此，与非复制表相比，`INSERT` 的延迟会略高一些。但如果你遵循建议，以批次方式插入数据，并将频率控制在每秒不超过一次 `INSERT`，就不会产生任何问题。用于协调一个 ZooKeeper 集群的整个 ClickHouse 集群，总计可支持每秒数百次 `INSERT`。数据插入的吞吐量 (即每秒插入的行数) 与非复制数据一样高。

对于非常大的集群，你可以为不同分片使用不同的 ZooKeeper 集群。不过，根据我们的经验，在大约 300 台服务器的生产集群中，这并未被证明是必要的。

复制是异步且多主的。`INSERT` 查询 (以及 `ALTER`) 可以发送到任何可用服务器。数据会先插入执行该查询的服务器，然后再复制到其他服务器。由于是异步的，最近插入的数据会在一段延迟后才出现在其他副本上。如果部分副本不可用，则会在它们恢复可用后写入数据。如果某个副本可用，那么这段延迟就是通过网络传输压缩数据块所需的时间。可通过设置 [background&#95;schedule&#95;pool&#95;size](/zh/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size) 来指定复制表执行后台任务的线程数量。

`ReplicatedMergeTree` 引擎为复制拉取操作使用单独的线程池。该线程池的大小受 [background&#95;fetches&#95;pool&#95;size](/zh/operations/server-configuration-parameters/settings#background_fetches_pool_size) 设置限制，并且可通过重启服务器进行调优。

默认情况下，一条 INSERT 查询只会等待来自一个副本的数据写入确认。如果数据只成功写入一个副本，而该副本所在的服务器随后不复存在，那么已存储的数据就会丢失。要启用从多个副本获取数据写入确认，请使用 `insert_quorum` 选项。

每个数据块都是原子写入的。INSERT 查询会被拆分为多个块，每个块最多 `max_insert_block_size = 1048576` 行。换句话说，如果 `INSERT` 查询少于 1048576 行，那么它会以原子方式执行。

数据块会被去重。对于同一个数据块的多次写入 (大小相同、包含相同行且顺序相同的数据块) ，该块只会被写入一次。这样做是因为在发生网络故障时，客户端应用程序可能无法确定数据是否已经写入数据库，因此只需重新执行 `INSERT` 查询即可。具有相同数据的 `INSERT` 发送到哪个副本并不重要。`INSERT` 是幂等的。去重参数由 [merge&#95;tree](/zh/operations/server-configuration-parameters/settings.md/#merge_tree) 服务器设置控制。

在复制过程中，只有待插入的源数据会通过网络传输。后续的数据转换 (合并) 会以相同方式在所有副本上协调并执行。这可以最大限度减少网络使用量，也就是说，当副本位于不同数据中心时，复制同样可以良好运行。 (请注意，在不同数据中心之间复制数据正是复制的主要目标。)

同一份数据可以有任意数量的副本。根据我们的经验，在生产环境中，一个相对可靠且方便的方案是使用双副本复制，并让每台服务器使用 RAID-5 或 RAID-6 (某些情况下使用 RAID-10) 。

系统会监控副本上的数据同步性，并能够在发生故障后恢复。故障转移可以是自动的 (当数据差异较小时) ，也可以是半自动的 (当数据差异过大时，这可能表明存在配置错误) 。

<div id="creating-replicated-tables">
  ## 创建复制表
</div>

:::note
在 ClickHouse Cloud 中，复制由系统自动处理。

请使用不带复制参数的 [`MergeTree`](/zh/engines/table-engines/mergetree-family/mergetree) 来创建表。系统会在内部将 [`MergeTree`](/zh/engines/table-engines/mergetree-family/mergetree) 改写为 [`SharedMergeTree`](/zh/cloud/reference/shared-merge-tree)，以实现复制和数据分布。

请避免使用 `ReplicatedMergeTree` 或指定复制参数，因为复制由平台统一管理。

:::

<div id="replicatedmergetree-parameters">
  ### Replicated*MergeTree 参数
</div>

| 参数                 | 说明                                                    |
| ------------------ | ----------------------------------------------------- |
| `zoo_path`         | ClickHouse Keeper 中该表的路径。                             |
| `replica_name`     | ClickHouse Keeper 中的副本名称。                             |
| `other_parameters` | 用于创建复制版的引擎参数，例如 `ReplacingMergeTree` 中的 `version` 参数。 |

示例：

```sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">
  <summary>已弃用语法示例</summary>

  ```sql
  CREATE TABLE table_name
  (
      EventDate DateTime,
      CounterID UInt32,
      UserID UInt32
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
  ```
</details>

如上例所示，这些参数可以包含 `{}` 中的替换项。替换后的值取自配置文件中的 [macros](/zh/operations/server-configuration-parameters/settings.md/#macros) 部分。

示例：

```xml
<macros>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

ClickHouse Keeper 中的表路径对每个复制表来说都应唯一。位于不同分片上的表应使用不同的路径。
在这种情况下，该路径由以下部分组成：

`/clickhouse/tables/` 是通用前缀。我们建议严格使用这个前缀。

`{shard}` 会展开为分片标识符。

`table_name` 是 ClickHouse Keeper 中该表对应节点的名称。建议将它设为与表名相同。之所以显式定义它，是因为它不同于表名，在执行 `RENAME` 查询后不会发生变化。
*提示*：你也可以在 `table_name` 前加上数据库名。例如：`db_name.table_name`

可以使用两个内置替换 `{database}` 和 `{table}`，它们会分别展开为表名和数据库名 (除非这些宏已在 `macros` 部分中定义) 。因此，ZooKeeper 路径可以指定为 `'/clickhouse/tables/{shard}/{database}/{table}'`。
使用这些内置替换时，要谨慎处理表重命名。ClickHouse Keeper 中的路径无法更改，而当表被重命名时，这些宏会展开为另一个路径，表将引用 ClickHouse Keeper 中不存在的路径，并进入只读模式。

副本名称用于标识同一张表的不同副本。你可以像示例中那样使用服务器名称。该名称只需在每个分片内唯一即可。

你可以显式定义这些参数，而不使用替换。这在测试和配置小型集群时可能更方便。不过，在这种情况下，你不能使用分布式 DDL 查询 (`ON CLUSTER`) 。

在处理大型集群时，我们建议使用替换，因为这样可以降低出错的概率。

你可以在服务器配置文件中为 `Replicated` 表引擎指定默认参数。例如：

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

在这种情况下，创建表时可省略参数：

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

这相当于：

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

在每个副本上执行 `CREATE TABLE` 查询。此查询会创建一个新的复制表，或为现有复制表添加一个新副本。

如果在其他副本上的该表已经包含一些数据之后再添加新副本，那么在运行该查询后，数据会从其他副本复制到新副本。换句话说，新副本会自行与其他副本同步。

要删除某个副本，请运行 `DROP TABLE`。不过，只会删除一个副本——即你执行该查询的服务器上的那个副本。

<div id="recovery-after-failures">
  ## 故障后的恢复
</div>

如果服务器启动时 ClickHouse Keeper 不可用，复制表会切换为只读模式。系统会定期尝试连接 ClickHouse Keeper。

如果在执行 `INSERT` 期间 ClickHouse Keeper 不可用，或在与 ClickHouse Keeper 交互时发生错误，系统就会抛出异常。

连接到 ClickHouse Keeper 后，系统会检查本地文件系统中的数据集是否与预期数据集一致 (这些信息存储在 ClickHouse Keeper 中) 。如果只是轻微不一致，系统会通过与副本同步数据来修复。

如果系统检测到损坏的数据分区片段 (文件大小不正确) 或无法识别的数据分区片段 (已写入文件系统但未记录在 ClickHouse Keeper 中的数据分区片段) ，就会将它们移动到 `已分离` 子目录中 (不会删除) 。任何缺失的数据分区片段都会从副本复制。

请注意，ClickHouse 不会执行任何破坏性操作，例如自动删除大量数据。

当服务器启动时 (或与 ClickHouse Keeper 建立新会话时) ，它只会检查所有文件的数量和大小。如果文件大小一致，但中间某处的字节发生了变化，系统不会立即检测到，而只会在尝试为 `SELECT` 查询读取数据时才发现。该查询会抛出校验和不匹配或压缩块大小不匹配的异常。在这种情况下，数据分区片段会被加入校验队列，并在必要时从副本复制。

如果本地数据集与预期数据集差异过大，就会触发安全机制。服务器会将此情况记入日志并拒绝启动。原因在于，这种情况可能表明存在配置错误，例如某个分片上的副本被误配置成另一个分片上的副本。不过，这一机制的阈值设置得相当低，因此在正常的故障恢复过程中也可能出现这种情况。在这种情况下，数据会以半自动方式恢复——通过 &quot;按下一个按钮&quot;。

要开始恢复，请在 ClickHouse Keeper 中创建节点 `/path_to_table/replica_name/flags/force_restore_data` 并写入任意内容，或者运行以下命令以恢复所有复制表：

```bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

然后重启服务器。启动时，服务器会删除这些标记并开始恢复。

<div id="recovery-after-complete-data-loss">
  ## 完全数据丢失后的恢复
</div>

如果某台服务器上的所有数据和元数据都已丢失，请按以下步骤恢复：

1. 在该服务器上安装 ClickHouse。如果使用了替换，请在包含分片标识符和副本标识符的配置文件中正确进行定义。
2. 如果存在未复制的表，并且这些表需要在各服务器之间手动复制，请从某个副本复制其数据 (位于目录 `/var/lib/clickhouse/data/db_name/table_name/`) 。
3. 从某个副本复制位于 `/var/lib/clickhouse/metadata/` 中的表定义。如果表定义中显式指定了分片或副本标识符，请将其改为与当前副本对应的值。 (或者，启动 server，并执行原本应写入 `/var/lib/clickhouse/metadata/` 下 .sql 文件中的所有 `ATTACH TABLE` queries。)
4. 要开始恢复，请创建 ClickHouse Keeper 节点 `/path_to_table/replica_name/flags/force_restore_data`，内容任意；或者运行以下命令以恢复所有复制表：`sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

然后启动 server (如果已在运行，则重启) 。数据将从各副本下载。

另一种恢复方式是从 ClickHouse Keeper 中删除丢失副本的信息 (`/path_to_table/replica_name`) ，然后按照“[创建复制表](#creating-replicated-tables)”中的说明重新创建该副本。

恢复期间不会限制网络带宽。如果要同时恢复多个副本，请注意这一点。

<div id="converting-from-mergetree-to-replicatedmergetree">
  ## 从 MergeTree 转换为 ReplicatedMergeTree
</div>

这里的 `MergeTree` 一词指 `MergeTree 家族` 中的所有表引擎，`ReplicatedMergeTree` 也是同样的用法。

如果你有一个通过手动方式复制的 `MergeTree` 表，可以将其转换为复制表。如果你已经在 `MergeTree` 表中收集了大量数据，而现在又想启用复制，就可能需要这样做。

[ATTACH TABLE ... AS REPLICATED](/zh/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) 语句允许将处于 `已分离` 状态的 `MergeTree` 表附加为 `ReplicatedMergeTree`。

如果在表的数据目录中设置了 `convert_to_replicated` 标志，则 `MergeTree` 表可在服务器重启时自动转换 (对于 `Atomic` 数据库，该目录为 `/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`) 。
创建一个空的 `convert_to_replicated` 文件后，该表会在下次服务器重启时以 replicated 方式加载。

此查询可用于获取表的数据路径。如果表有多个数据路径，则必须使用第一个。

```sql
SELECT data_paths FROM system.tables WHERE table = 'table_name' AND database = 'database_name';
```

请注意，ReplicatedMergeTree 表将使用 `default_replica_path` 和 `default_replica_name` 设置的值创建。
要在其他副本上创建转换后的表，您需要在 `ReplicatedMergeTree` 引擎的第一个参数中显式指定其路径。可以使用以下查询获取该路径。

```sql
SELECT zookeeper_path FROM system.replicas WHERE table = 'table_name';
```

也可以手动执行此操作。

如果各个副本上的数据不一致，请先同步，或者删除除一个副本外所有副本上的这些数据。

先将现有的 MergeTree 表重命名，再使用原名称创建一个 `ReplicatedMergeTree` 表。
将旧表中的数据移到新表数据所在目录 (`/var/lib/clickhouse/data/db_name/table_name/`) 内的 `已分离` 子目录中。
然后在其中一个副本上运行 `ALTER TABLE ATTACH PARTITION`，将这些数据分区片段添加到工作集。

<div id="converting-from-replicatedmergetree-to-mergetree">
  ## 从 ReplicatedMergeTree 转换为 MergeTree
</div>

使用 [ATTACH TABLE ... AS NOT REPLICATED](/zh/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) 语句，可以在单台服务器上将已分离的 `ReplicatedMergeTree` 表附加为 `MergeTree`。

另一种方法需要重启服务器。先创建一个名称不同的 MergeTree 表。将存放 `ReplicatedMergeTree` 表数据的目录中的所有数据移动到新表的数据目录。然后删除 `ReplicatedMergeTree` 表并重启服务器。

如果你想在不启动服务器的情况下移除 `ReplicatedMergeTree` 表：

* 删除元数据目录 (`/var/lib/clickhouse/metadata/`) 中对应的 `.sql` 文件。
* 删除 ClickHouse Keeper 中对应的路径 (`/path_to_table/replica_name`) 。

完成后，你可以启动服务器，创建一个 `MergeTree` 表，将数据移动到其目录中，然后再次重启服务器。

<div id="recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged">
  ## ClickHouse Keeper 集群中的元数据丢失或损坏时的恢复
</div>

如果 ClickHouse Keeper 中的数据丢失或损坏，您可以按上文所述，将数据移动到非复制表中进行保存。

**另请参阅**

* [background&#95;schedule&#95;pool&#95;size](/zh/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)
* [background&#95;fetches&#95;pool&#95;size](/zh/operations/server-configuration-parameters/settings.md/#background_fetches_pool_size)
* [execute&#95;merges&#95;on&#95;single&#95;replica&#95;time&#95;threshold](/zh/operations/settings/merge-tree-settings#execute_merges_on_single_replica_time_threshold)
* [max&#95;replicated&#95;fetches&#95;network&#95;bandwidth](/zh/operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth)
* [max&#95;replicated&#95;sends&#95;network&#95;bandwidth](/zh/operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth)