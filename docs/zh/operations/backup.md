---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: "\u6570\u636E\u5907\u4EFD"
---

# 数据备份 {#data-backup}

尽管[副本](../engines/table-engines/mergetree-family/replication.md) 可以预防硬件错误带来的数据丢失, 但是它不能防止人为操作的错误: 意外删除数据, 删除错误的 table 或者删除错误 cluster 上的 table, 可以导致错误数据处理错误或者数据损坏的 bugs. 这类意外可能会影响所有的副本. ClickHouse 有内建的保障措施可以预防一些错误 — 例如, 默认情况下[您不能使用类似MergeTree的引擎删除包含超过50Gb数据的表](https://github.com/ClickHouse/ClickHouse/blob/v18.14.18-stable/programs/server/config.xml#L322-L330). 但是，这些保障措施不能涵盖所有可能的情况，并且可以规避。

为了有效地减少可能的人为错误，您应该 **提前**准备备份和还原数据的策略.

不同公司有不同的可用资源和业务需求，因此没有适合各种情况的ClickHouse备份和恢复通用解决方案。 适用于 1GB 的数据的方案可能并不适用于几十 PB 数据的情况。 有多种可能的并有自己优缺点的方法，这将在下面讨论。 好的主意是同时结合使用多种方法而不是仅使用一种，这样可以弥补不同方法各自的缺点。

!!! note "注"
    请记住，如果您备份了某些内容并且从未尝试过还原它，那么当您实际需要它时（或者至少需要比业务能够容忍的时间更长），恢复可能无法正常工作。 因此，无论您选择哪种备份方法，请确保自动还原过程，并定期在备用ClickHouse群集上练习。

## 将源数据复制到其他地方 {#duplicating-source-data-somewhere-else}

通常被聚集到ClickHouse的数据是通过某种持久队列传递的，例如 [Apache Kafka](https://kafka.apache.org). 在这种情况下，可以配置一组额外的订阅服务器，这些订阅服务器将在写入ClickHouse时读取相同的数据流，并将其存储在冷存储中。 大多数公司已经有一些默认的推荐冷存储，可能是对象存储或分布式文件系统，如 [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## 文件系统快照 {#filesystem-snapshots}

某些本地文件系统提供快照功能（例如, [ZFS](https://en.wikipedia.org/wiki/ZFS)），但它们可能不是提供实时查询的最佳选择。 一个可能的解决方案是使用这种文件系统创建额外的副本，并将它们从 [分布](../engines/table-engines/special/distributed.md) 用于以下目的的表 `SELECT` 查询。 任何修改数据的查询都无法访问此类副本上的快照。 作为奖励，这些副本可能具有特殊的硬件配置，每个服务器附加更多的磁盘，这将是经济高效的。

## clickhouse-copier {#clickhouse-copier}

[clickhouse-copier](utilities/clickhouse-copier.md) 是一个多功能工具，最初创建用于重新分片pb大小的表。 因为它可以在ClickHouse表和集群之间可靠地复制数据，所以它还可用于备份和还原数据。

对于较小的数据量，一个简单的 `INSERT INTO ... SELECT ...` 到远程表也可以工作。

## 部件操作 {#manipulations-with-parts}

ClickHouse允许使用 `ALTER TABLE ... FREEZE PARTITION ...` 查询以创建表分区的本地副本。 这是利用硬链接(hardlink)到 `/var/lib/clickhouse/shadow/` 文件夹中实现的，所以它通常不会占用旧数据的额外磁盘空间。 创建的文件副本不由ClickHouse服务器处理，所以你可以把它们留在那里：你将有一个简单的备份，不需要任何额外的外部系统，但它仍然会容易出现硬件问题。 出于这个原因，最好将它们远程复制到另一个位置，然后删除本地副本。 分布式文件系统和对象存储仍然是一个不错的选择，但是具有足够大容量的正常附加文件服务器也可以工作（在这种情况下，传输将通过网络文件系统 [rsync](https://en.wikipedia.org/wiki/Rsync)).

数据可以使用 `ALTER TABLE ... ATTACH PARTITION ...` 从备份中恢复。

有关与分区操作相关的查询的详细信息，请参阅 [更改文档](../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

第三方工具可用于自动化此方法: [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup).

[原始文章](https://clickhouse.tech/docs/en/operations/backup/) <!--hide-->
