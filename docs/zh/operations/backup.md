---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: "\u6570\u636E\u5907\u4EFD"
---

# 数据备份 {#data-backup}

碌莽禄While: [复制](../engines/table-engines/mergetree-family/replication.md) provides protection from hardware failures, it does not protect against human errors: accidental deletion of data, deletion of the wrong table or a table on the wrong cluster, and software bugs that result in incorrect data processing or data corruption. In many cases mistakes like these will affect all replicas. ClickHouse has built-in safeguards to prevent some types of mistakes — for example, by default [您不能使用类似MergeTree的引擎删除包含超过50Gb数据的表](https://github.com/ClickHouse/ClickHouse/blob/v18.14.18-stable/programs/server/config.xml#L322-L330). 但是，这些保障措施并不涵盖所有可能的情况，可以规避。

为了有效地减少可能的人为错误，您应该仔细准备备份和还原数据的策略 **提前**.

每家公司都有不同的可用资源和业务需求，因此没有适合各种情况的ClickHouse备份和恢复通用解决方案。 什么适用于一千兆字节的数据可能不会为几十pb的工作。 有多种可能的方法有自己的优点和缺点，这将在下面讨论。 这是一个好主意，使用几种方法，而不是只是一个，以弥补其各种缺点。

!!! note "注"
    请记住，如果您备份了某些内容并且从未尝试过还原它，那么当您实际需要它时（或者至少需要比业务能够容忍的时间更长），恢复可能无法正常工作。 因此，无论您选择哪种备份方法，请确保自动还原过程，并定期在备用ClickHouse群集上练习。

## 将源数据复制到其他地方 {#duplicating-source-data-somewhere-else}

通常被摄入到ClickHouse的数据是通过某种持久队列传递的，例如 [Apache Kafka](https://kafka.apache.org). 在这种情况下，可以配置一组额外的订阅服务器，这些订阅服务器将在写入ClickHouse时读取相同的数据流，并将其存储在冷存储中。 大多数公司已经有一些默认的推荐冷存储，可能是对象存储或分布式文件系统，如 [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## 文件系统快照 {#filesystem-snapshots}

某些本地文件系统提供快照功能（例如, [ZFS](https://en.wikipedia.org/wiki/ZFS)），但它们可能不是提供实时查询的最佳选择。 一个可能的解决方案是使用这种文件系统创建额外的副本，并将它们从 [分布](../engines/table-engines/special/distributed.md) 用于以下目的的表 `SELECT` 查询。 任何修改数据的查询都无法访问此类副本上的快照。 作为奖励，这些副本可能具有特殊的硬件配置，每个服务器附加更多的磁盘，这将是经济高效的。

## ﾂ环板-ｮﾂ嘉ｯﾂ偲 {#clickhouse-copier}

[ﾂ环板-ｮﾂ嘉ｯﾂ偲](utilities/clickhouse-copier.md) 是一个多功能工具，最初创建用于重新分片pb大小的表。 它还可用于备份和还原目的，因为它可以在ClickHouse表和集群之间可靠地复制数据。

对于较小的数据量，一个简单的 `INSERT INTO ... SELECT ...` 到远程表也可以工作。

## 部件操作 {#manipulations-with-parts}

ClickHouse允许使用 `ALTER TABLE ... FREEZE PARTITION ...` 查询以创建表分区的本地副本。 这是使用硬链接来实现 `/var/lib/clickhouse/shadow/` 文件夹中，所以它通常不会占用旧数据的额外磁盘空间。 创建的文件副本不由ClickHouse服务器处理，所以你可以把它们留在那里：你将有一个简单的备份，不需要任何额外的外部系统，但它仍然会容易出现硬件问题。 出于这个原因，最好将它们远程复制到另一个位置，然后删除本地副本。 分布式文件系统和对象存储仍然是一个不错的选择，但是具有足够大容量的正常附加文件服务器也可以工作（在这种情况下，传输将通过网络文件系统 [rsync](https://en.wikipedia.org/wiki/Rsync)).

有关与分区操作相关的查询的详细信息，请参阅 [更改文档](../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

第三方工具可用于自动化此方法: [ﾂ环板backupｮﾂ嘉ｯﾂ偲](https://github.com/AlexAkulov/clickhouse-backup).

[原始文章](https://clickhouse.tech/docs/en/operations/backup/) <!--hide-->
