---
description: 'Log 引擎家族文档'
sidebar_label: 'Log 家族'
sidebar_position: 20
slug: /engines/table-engines/log-family/
title: 'Log 引擎家族'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine-family">
  # Log 表引擎家族
</div>

<CloudNotSupportedBadge />

这些引擎适用于需要快速写入许多小表 (最多约 100 万行) ，并在之后整体读取的场景。

该家族包含以下引擎：

| Log 引擎                                                      |
| ----------------------------------------------------------- |
| [StripeLog](/zh/engines/table-engines/log-family/stripelog.md) |
| [Log](/zh/engines/table-engines/log-family/log.md)             |
| [TinyLog](/zh/engines/table-engines/log-family/tinylog.md)     |

`Log` 家族表引擎可以将数据存储到 [HDFS](/zh/engines/table-engines/integrations/hdfs) 或 [S3](/zh/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3) 分布式文件系统中。

:::warning 此引擎并不适用于日志数据。
尽管名称如此，*Log 表引擎并不是为存储日志数据而设计的。它们仅适用于需要快速写入的小数据量场景。
:::

<div id="common-properties">
  ## 通用属性
</div>

引擎：

* 将数据存储在磁盘上。

* 写入时将数据追加到文件末尾。

* 支持并发数据访问的锁机制。

  在执行 `INSERT` 查询期间，表会被锁定，其他读取和写入数据的查询都需要等待表解锁。如果没有写入数据的查询，则可以并发执行任意数量的数据读取查询。

* 不支持[变更](/zh/sql-reference/statements/alter#mutations)。

* 不支持索引。

  这意味着，对数据范围执行的 `SELECT` 查询效率不高。

* 不以原子方式写入数据。

  如果写入过程中发生故障，例如服务器异常关闭，你可能会得到一个包含损坏数据的表。

<div id="differences">
  ## 区别
</div>

`TinyLog` 引擎是该家族中最简单的一种，功能最弱，效率也最低。`TinyLog` 引擎不支持在单个查询中通过多个线程并行读取数据。与该家族中其他支持在单个查询中并行读取的引擎相比，它读取数据的速度更慢；并且由于它将每一列存储在单独的文件中，因此使用的文件描述符数量几乎与 `Log` 引擎一样多。仅应在简单场景中使用它。

`Log` 和 `StripeLog` 引擎支持并行读取数据。读取数据时，ClickHouse 会使用多个线程。每个线程处理一个单独的数据块。`Log` 引擎为表中的每一列使用单独的文件。`StripeLog` 将所有数据存储在一个文件中。因此，`StripeLog` 引擎使用的文件描述符更少，但 `Log` 引擎在读取数据时效率更高。