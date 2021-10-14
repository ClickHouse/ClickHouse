# 数据副本 {#table_engines-replication}

只有 MergeTree 系列里的表可支持副本：

-   ReplicatedMergeTree
-   ReplicatedSummingMergeTree
-   ReplicatedReplacingMergeTree
-   ReplicatedAggregatingMergeTree
-   ReplicatedCollapsingMergeTree
-   ReplicatedVersionedCollapsingMergetree
-   ReplicatedGraphiteMergeTree

副本是表级别的，不是整个服务器级的。所以，服务器里可以同时有复制表和非复制表。

副本不依赖分片。每个分片有它自己的独立副本。

对于 `INSERT` 和 `ALTER` 语句操作数据的会在压缩的情况下被复制（更多信息，看 [ALTER](../../../engines/table-engines/mergetree-family/replication.md#query_language_queries_alter) ）。

而 `CREATE`，`DROP`，`ATTACH`，`DETACH` 和 `RENAME` 语句只会在单个服务器上执行，不会被复制。

-   `The CREATE TABLE` 在运行此语句的服务器上创建一个新的可复制表。如果此表已存在其他服务器上，则给该表添加新副本。
-   `The DROP TABLE` 删除运行此查询的服务器上的副本。
-   `The RENAME` 重命名一个副本。换句话说，可复制表不同的副本可以有不同的名称。

要使用副本，需在配置文件中设置 ZooKeeper 集群的地址。例如：

``` xml
<zookeeper>
    <node index="1">
        <host>example1</host>
        <port>2181</port>
    </node>
    <node index="2">
        <host>example2</host>
        <port>2181</port>
    </node>
    <node index="3">
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

需要 ZooKeeper 3.4.5 或更高版本。

你可以配置任何现有的 ZooKeeper 集群，系统会使用里面的目录来存取元数据（该目录在创建可复制表时指定）。

如果配置文件中没有设置 ZooKeeper ，则无法创建复制表，并且任何现有的复制表都将变为只读。

`SELECT` 查询并不需要借助 ZooKeeper ，复本并不影响 `SELECT` 的性能，查询复制表与非复制表速度是一样的。查询分布式表时，ClickHouse的处理方式可通过设置 [max_replica_delay_for_distributed_queries](../../../operations/settings/settings.md#settings-max_replica_delay_for_distributed_queries) 和 [fallback_to_stale_replicas_for_distributed_queries](../../../operations/settings/settings.md) 修改。

对于每个 `INSERT` 语句，会通过几个事务将十来个记录添加到 ZooKeeper。（确切地说，这是针对每个插入的数据块; 每个 INSERT 语句的每 `max_insert_block_size = 1048576` 行和最后剩余的都各算作一个块。）相比非复制表，写 zk 会导致 `INSERT` 的延迟略长一些。但只要你按照建议每秒不超过一个 `INSERT` 地批量插入数据，不会有任何问题。一个 ZooKeeper 集群能给整个 ClickHouse 集群支撑协调每秒几百个 `INSERT`。数据插入的吞吐量（每秒的行数）可以跟不用复制的数据一样高。

对于非常大的集群，你可以把不同的 ZooKeeper 集群用于不同的分片。然而，即使 Yandex.Metrica 集群（大约300台服务器）也证明还不需要这么做。

复制是多主异步。 `INSERT` 语句（以及 `ALTER` ）可以发给任意可用的服务器。数据会先插入到执行该语句的服务器上，然后被复制到其他服务器。由于它是异步的，在其他副本上最近插入的数据会有一些延迟。如果部分副本不可用，则数据在其可用时再写入。副本可用的情况下，则延迟时长是通过网络传输压缩数据块所需的时间。

默认情况下，INSERT 语句仅等待一个副本写入成功后返回。如果数据只成功写入一个副本后该副本所在的服务器不再存在，则存储的数据会丢失。要启用数据写入多个副本才确认返回，使用 `insert_quorum` 选项。

单个数据块写入是原子的。 INSERT 的数据按每块最多 `max_insert_block_size = 1048576` 行进行分块，换句话说，如果 `INSERT` 插入的行少于 1048576，则该 INSERT 是原子的。

数据块会去重。对于被多次写的相同数据块（大小相同且具有相同顺序的相同行的数据块），该块仅会写入一次。这样设计的原因是万一在网络故障时客户端应用程序不知道数据是否成功写入DB，此时可以简单地重复 `INSERT` 。把相同的数据发送给多个副本 INSERT 并不会有问题。因为这些 `INSERT` 是完全相同的（会被去重）。去重参数参看服务器设置 [merge_tree](../../../operations/server-configuration-parameters/settings.md) 。（注意：Replicated\*MergeTree 才会去重，不需要 zookeeper 的不带 MergeTree 不会去重）

在复制期间，只有要插入的源数据通过网络传输。进一步的数据转换（合并）会在所有副本上以相同的方式进行处理执行。这样可以最大限度地减少网络使用，这意味着即使副本在不同的数据中心，数据同步也能工作良好。（能在不同数据中心中的同步数据是副本机制的主要目标。）

你可以给数据做任意多的副本。Yandex.Metrica 在生产中使用双副本。某一些情况下，给每台服务器都使用 RAID-5 或 RAID-6 和 RAID-10。是一种相对可靠和方便的解决方案。

系统会监视副本数据同步情况，并能在发生故障后恢复。故障转移是自动的（对于小的数据差异）或半自动的（当数据差异很大时，这可能意味是有配置错误）。

## 创建复制表 {#creating-replicated-tables}

在表引擎名称上加上 `Replicated` 前缀。例如：`ReplicatedMergeTree`。

**Replicated\*MergeTree 参数**

-   `zoo_path` — ZooKeeper 中该表的路径。
-   `replica_name` — ZooKeeper 中的该表的副本名称。

示例:

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}')
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
```

已弃用的建表语法示例：

``` sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)
```

如上例所示，这些参数可以包含宏替换的占位符，即大括号的部分。它们会被替换为配置文件里 ‘macros’ 那部分配置的值。示例：

``` xml
<macros>
    <layer>05</layer>
    <shard>02</shard>
    <replica>example05-02-1.yandex.ru</replica>
</macros>
```

«ZooKeeper 中该表的路径»对每个可复制表都要是唯一的。不同分片上的表要有不同的路径。
这种情况下，路径包含下面这些部分：

`/clickhouse/tables/` 是公共前缀，我们推荐使用这个。

`{layer}-{shard}` 是分片标识部分。在此示例中，由于 Yandex.Metrica 集群使用了两级分片，所以它是由两部分组成的。但对于大多数情况来说，你只需保留 {shard} 占位符即可，它会替换展开为分片标识。

`table_name` 是该表在 ZooKeeper 中的名称。使其与 ClickHouse 中的表名相同比较好。 这里它被明确定义，跟 ClickHouse 表名不一样，它并不会被 RENAME 语句修改。
*HINT*：你可以在前面添加一个数据库名称 `table_name` 也是 例如。 `db_name.table_name`

副本名称用于标识同一个表分片的不同副本。你可以使用服务器名称，如上例所示。同个分片中不同副本的副本名称要唯一。

你也可以显式指定这些参数，而不是使用宏替换。对于测试和配置小型集群这可能会很方便。但是，这种情况下，则不能使用分布式 DDL 语句（`ON CLUSTER`）。

使用大型集群时，我们建议使用宏替换，因为它可以降低出错的可能性。

在每个副本服务器上运行 `CREATE TABLE` 查询。将创建新的复制表，或给现有表添加新副本。

如果其他副本上已包含了某些数据，在表上添加新副本，则在运行语句后，数据会从其他副本复制到新副本。换句话说，新副本会与其他副本同步。

要删除副本，使用 `DROP TABLE`。但它只删除那个 – 位于运行该语句的服务器上的副本。

## 故障恢复 {#gu-zhang-hui-fu}

如果服务器启动时 ZooKeeper 不可用，则复制表会切换为只读模式。系统会定期尝试去连接 ZooKeeper。

如果在 `INSERT` 期间 ZooKeeper 不可用，或者在与 ZooKeeper 交互时发生错误，则抛出异常。

连接到 ZooKeeper 后，系统会检查本地文件系统中的数据集是否与预期的数据集（ ZooKeeper 存储此信息）一致。如果存在轻微的不一致，系统会通过与副本同步数据来解决。

如果系统检测到损坏的数据片段（文件大小错误）或无法识别的片段（写入文件系统但未记录在 ZooKeeper 中的部分），则会把它们移动到 ‘detached’ 子目录（不会删除）。而副本中其他任何缺少的但正常数据片段都会被复制同步。

注意，ClickHouse 不会执行任何破坏性操作，例如自动删除大量数据。

当服务器启动（或与 ZooKeeper 建立新会话）时，它只检查所有文件的数量和大小。 如果文件大小一致但中间某处已有字节被修改过，不会立即被检测到，只有在尝试读取 `SELECT` 查询的数据时才会检测到。该查询会引发校验和不匹配或压缩块大小不一致的异常。这种情况下，数据片段会添加到验证队列中，并在必要时从其他副本中复制。

如果本地数据集与预期数据的差异太大，则会触发安全机制。服务器在日志中记录此内容并拒绝启动。这种情况很可能是配置错误，例如，一个分片上的副本意外配置为别的分片上的副本。然而，此机制的阈值设置得相当低，在正常故障恢复期间可能会出现这种情况。在这种情况下，数据恢复则是半自动模式，通过用户主动操作触发。

要触发启动恢复，可在 ZooKeeper 中创建节点 `/path_to_table/replica_name/flags/force_restore_data`，节点值可以是任何内容，或运行命令来恢复所有的可复制表：

``` bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

然后重启服务器。启动时，服务器会删除这些标志并开始恢复。

## 在数据完全丢失后的恢复 {#zai-shu-ju-wan-quan-diu-shi-hou-de-hui-fu}

如果其中一个服务器的所有数据和元数据都消失了，请按照以下步骤进行恢复：

1.  在服务器上安装 ClickHouse。在包含分片标识符和副本的配置文件中正确定义宏配置，如果有用到的话，
2.  如果服务器上有非复制表则必须手动复制，可以从副本服务器上（在 `/var/lib/clickhouse/data/db_name/table_name/` 目录中）复制它们的数据。
3.  从副本服务器上中复制位于 `/var/lib/clickhouse/metadata/` 中的表定义信息。如果在表定义信息中显式指定了分片或副本标识符，请更正它以使其对应于该副本。（另外，启动服务器，然后会在 `/var/lib/clickhouse/metadata/` 中的.sql文件中生成所有的 `ATTACH TABLE` 语句。）
    4.要开始恢复，ZooKeeper 中创建节点 `/path_to_table/replica_name/flags/force_restore_data`，节点内容不限，或运行命令来恢复所有复制的表：`sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

然后启动服务器（如果它已运行则重启）。数据会从副本中下载。

另一种恢复方式是从 ZooKeeper（`/path_to_table/replica_name`）中删除有数据丢的副本的所有元信息，然后再按照«[创建可复制表](#creating-replicated-tables)»中的描述重新创建副本。

恢复期间的网络带宽没有限制。特别注意这一点，尤其是要一次恢复很多副本。

## MergeTree 转换为 ReplicatedMergeTree {#mergetree-zhuan-huan-wei-replicatedmergetree}

我们使用 `MergeTree` 来表示 `MergeTree系列` 中的所有表引擎，`ReplicatedMergeTree` 同理。

如果你有一个手动同步的 `MergeTree` 表，您可以将其转换为可复制表。如果你已经在 `MergeTree` 表中收集了大量数据，并且现在要启用复制，则可以执行这些操作。

如果各个副本上的数据不一致，则首先对其进行同步，或者除保留的一个副本外，删除其他所有副本上的数据。

重命名现有的 MergeTree 表，然后使用旧名称创建 `ReplicatedMergeTree` 表。
将数据从旧表移动到新表（`/var/lib/clickhouse/data/db_name/table_name/`）目录内的 ‘detached’ 目录中。
然后在其中一个副本上运行`ALTER TABLE ATTACH PARTITION`，将这些数据片段添加到工作集中。

## ReplicatedMergeTree 转换为 MergeTree {#replicatedmergetree-zhuan-huan-wei-mergetree}

使用其他名称创建 MergeTree 表。将具有`ReplicatedMergeTree`表数据的目录中的所有数据移动到新表的数据目录中。然后删除`ReplicatedMergeTree`表并重新启动服务器。

如果你想在不启动服务器的情况下清除 `ReplicatedMergeTree` 表：

-   删除元数据目录中的相应 `.sql` 文件（`/var/lib/clickhouse/metadata/`）。
-   删除 ZooKeeper 中的相应路径（`/path_to_table/replica_name`）。

之后，你可以启动服务器，创建一个 `MergeTree` 表，将数据移动到其目录，然后重新启动服务器。

## 当 ZooKeeper 集群中的元数据丢失或损坏时恢复方法 {#dang-zookeeper-ji-qun-zhong-de-yuan-shu-ju-diu-shi-huo-sun-pi-shi-hui-fu-fang-fa}

如果 ZooKeeper 中的数据丢失或损坏，如上所述，你可以通过将数据转移到非复制表来保存数据。

[来源文章](https://clickhouse.tech/docs/en/operations/table_engines/replication/) <!--hide-->
