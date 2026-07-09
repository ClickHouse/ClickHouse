---
description: '表引擎文档'
slug: /engines/table-engines/
toc_folder_title: '表引擎'
toc_priority: 26
toc_title: '简介'
title: '表引擎'
doc_type: 'reference'
---

表引擎 (表的类型) 决定了：

* 数据如何存储、存储在何处、写入到哪里，以及从哪里读取。
* 支持哪些查询，以及支持方式。
* 数据的并发访问。
* 是否使用索引 (如果有) 。
* 是否支持多线程请求执行。
* 数据复制参数。

<div id="engine-families">
  ## 引擎系列
</div>

<div id="mergetree">
  ### MergeTree
</div>

最通用、功能最全面的表引擎，适用于高负载任务。这些引擎的共同特性是支持快速插入数据，并随后在后台对数据进行处理。`MergeTree` 家族引擎支持数据复制 (通过引擎的 [Replicated*](/zh/engines/table-engines/mergetree-family/replication) 版本) 、分区、二级数据跳过索引，以及其他引擎不支持的功能。

该家族中的引擎：

| MergeTree 引擎                                                                                         |
| ---------------------------------------------------------------------------------------------------- |
| [MergeTree](/zh/engines/table-engines/mergetree-family/mergetree)                                       |
| [ReplacingMergeTree](/zh/engines/table-engines/mergetree-family/replacingmergetree)                     |
| [SummingMergeTree](/zh/engines/table-engines/mergetree-family/summingmergetree)                         |
| [AggregatingMergeTree](/zh/engines/table-engines/mergetree-family/aggregatingmergetree)                 |
| [CollapsingMergeTree](/zh/engines/table-engines/mergetree-family/collapsingmergetree)                   |
| [VersionedCollapsingMergeTree](/zh/engines/table-engines/mergetree-family/versionedcollapsingmergetree) |
| [GraphiteMergeTree](/zh/engines/table-engines/mergetree-family/graphitemergetree)                       |
| [CoalescingMergeTree](/zh/engines/table-engines/mergetree-family/coalescingmergetree)                   |

<div id="log">
  ### Log
</div>

功能最简的轻量级 [引擎](../../engines/table-engines/log-family/index.md)。当你需要快速写入许多小表 (最多约 100 万行) ，并在之后按整体读取时，它们的效率最高。

该家族中的引擎：

| Log 引擎                                                   |
| -------------------------------------------------------- |
| [TinyLog](/zh/engines/table-engines/log-family/tinylog)     |
| [StripeLog](/zh/engines/table-engines/log-family/stripelog) |
| [Log](/zh/engines/table-engines/log-family/log)             |

<div id="integration-engines">
  ### 集成引擎
</div>

用于与其他数据存储和处理系统交互的引擎。

该家族中的引擎：

| 集成引擎                                                                            |
| ------------------------------------------------------------------------------- |
| [ODBC](../../engines/table-engines/integrations/odbc.md)                        |
| [JDBC](../../engines/table-engines/integrations/jdbc.md)                        |
| [MySQL](../../engines/table-engines/integrations/mysql.md)                      |
| [MongoDB](../../engines/table-engines/integrations/mongodb.md)                  |
| [Redis](../../engines/table-engines/integrations/redis.md)                      |
| [HDFS](../../engines/table-engines/integrations/hdfs.md)                        |
| [S3](../../engines/table-engines/integrations/s3.md)                            |
| [Kafka](../../engines/table-engines/integrations/kafka.md)                      |
| [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) |
| [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)                |
| [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)            |
| [S3Queue](../../engines/table-engines/integrations/s3queue.md)                  |
| [TimeSeries](../../engines/table-engines/integrations/time-series.md)           |

<div id="special-engines">
  ### 特殊引擎
</div>

该家族中的引擎：

| 特殊引擎                                                      |
| --------------------------------------------------------- |
| [Distributed](/zh/engines/table-engines/special/distributed) |
| [字典](/zh/engines/table-engines/special/dictionary)           |
| [Merge](/zh/engines/table-engines/special/merge)             |
| [Executable](/zh/engines/table-engines/special/executable)   |
| [File 表引擎](/zh/engines/table-engines/special/file)           |
| [Null](/zh/engines/table-engines/special/null)               |
| [Set](/zh/engines/table-engines/special/set)                 |
| [Join](/zh/engines/table-engines/special/join)               |
| [URL](/zh/engines/table-engines/special/url)                 |
| [View](/zh/engines/table-engines/special/view)               |
| [Memory](/zh/engines/table-engines/special/memory)           |
| [Buffer](/zh/engines/table-engines/special/buffer)           |
| [外部数据](/zh/engines/table-engines/special/external-data)      |
| [GenerateRandom](/zh/engines/table-engines/special/generate) |
| [KeeperMap](/zh/engines/table-engines/special/keeper-map)    |
| [FileLog](/zh/engines/table-engines/special/filelog)         |

<div id="table_engines-virtual_columns">
  ## 虚拟列
</div>

虚拟列是定义在引擎源代码中的表引擎内置属性。

你不应在 `CREATE TABLE` 查询中指定虚拟列，也无法在 `SHOW CREATE TABLE` 和 `DESCRIBE TABLE` 的查询结果中看到它们。虚拟列也是只读的，因此不能向虚拟列插入数据。

要从虚拟列中选择数据，必须在 `SELECT` 查询中指定其名称。`SELECT *` 不会返回虚拟列中的值。

如果你创建的表中有一个列与某个表虚拟列同名，则该虚拟列将无法访问。我们不建议这样做。为避免冲突，虚拟列名通常以下划线作为前缀。

* `_table` — 包含读取数据的来源表名称。类型：[String](../../sql-reference/data-types/string.md)。

  无论使用哪种表引擎，每个表都包含一个名为 `_table` 的通用虚拟列。

  查询使用 Merge table engine 的表时，你可以在 `WHERE/PREWHERE` 子句中为 `_table` 设置常量条件 (例如，`WHERE _table='xyz'`) 。在这种情况下，只会读取满足 `_table` 条件的那些表，因此 `_table` 列可充当索引。

  使用 `SELECT ... FROM (... UNION ALL ...)` 这种格式的查询时，我们可以通过指定 `_table` 列来确定返回的行实际来自哪个表。