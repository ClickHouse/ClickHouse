---
slug: /zh/operations/backup
sidebar_position: 49
sidebar_label: "\u6570\u636E\u5907\u4EFD"
---

# 数据备份 {#data-backup}

尽管 [副本](../engines/table-engines/mergetree-family/replication.md) 可以提供针对硬件的错误防护, 但是它不能预防人为操作失误: 数据的意外删除, 错误表的删除或者错误集群上表的删除, 以及导致错误数据处理或者数据损坏的软件bug. 在很多案例中，这类意外可能会影响所有的副本. ClickHouse 有内置的保护措施可以预防一些错误 — 例如, 默认情况下 [不能人工删除使用带有MergeTree引擎且包含超过50Gb数据的表](server-configuration-parameters/settings.md#max-table-size-to-drop). 但是，这些保护措施不能覆盖所有可能情况，并且这些措施可以被绕过。

为了有效地减少可能的人为错误，您应该 **提前** 仔细的准备备份和数据还原的策略.

不同公司有不同的可用资源和业务需求，因此不存在一个通用的解决方案可以应对各种情况下的ClickHouse备份和恢复。 适用于 1GB 数据的方案可能并不适用于几十 PB 数据的情况。 有多种具备各自优缺点的可能方法，将在下面对其进行讨论。最好使用几种方法而不是仅仅使用一种方法来弥补它们的各种缺点。。

:::note
需要注意的是，如果您备份了某些内容并且从未尝试过还原它，那么当您实际需要它时可能无法正常恢复（或者至少需要的时间比业务能够容忍的时间更长）。 因此，无论您选择哪种备份方法，请确保自动还原过程，并定期在备用ClickHouse群集上演练。
:::

## 将源数据复制到其它地方 {#duplicating-source-data-somewhere-else}

通常摄入到ClickHouse的数据是通过某种持久队列传递的，例如 [Apache Kafka](https://kafka.apache.org). 在这种情况下，可以配置一组额外的订阅服务器，这些订阅服务器将在写入ClickHouse时读取相同的数据流，并将其存储在冷存储中。 大多数公司已经有一些默认推荐的冷存储，可能是对象存储或分布式文件系统，如 [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## 文件系统快照 {#filesystem-snapshots}

某些本地文件系统提供快照功能（例如, [ZFS](https://en.wikipedia.org/wiki/ZFS)），但它们可能不是提供实时查询的最佳选择。 一个可能的解决方案是使用这种文件系统创建额外的副本，并将它们与用于`SELECT` 查询的 [分布式](../engines/table-engines/special/distributed.md) 表分离。 任何修改数据的查询都无法访问此类副本上的快照。 作为回报，这些副本可能具有特殊的硬件配置，每个服务器附加更多的磁盘，这将是经济高效的。

## part操作 {#manipulations-with-parts}

ClickHouse允许使用 `ALTER TABLE ... FREEZE PARTITION ...` 查询以创建表分区的本地副本。 这是利用硬链接(hardlink)到 `/var/lib/clickhouse/shadow/` 文件夹中实现的，所以它通常不会因为旧数据而占用额外的磁盘空间。 创建的文件副本不由ClickHouse服务器处理，所以你可以把它们留在那里：你将有一个简单的备份，不需要任何额外的外部系统，但它仍然容易出现硬件问题。 出于这个原因，最好将它们远程复制到另一个位置，然后删除本地副本。 分布式文件系统和对象存储仍然是一个不错的选择，但是具有足够大容量的正常附加文件服务器也可以工作（在这种情况下，传输将通过网络文件系统或者也许是 [rsync](https://en.wikipedia.org/wiki/Rsync) 来进行).

数据可以使用 `ALTER TABLE ... ATTACH PARTITION ...` 从备份中恢复。

有关与分区操作相关的查询的详细信息，请参阅 [更改文档](../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

第三方工具可用于自动化此方法: [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup).
