---
description: '将数据缓存在 RAM 中，并定期将其刷新到另一张表。在读取时，会同时从缓冲区和另一张表中读取数据。'
sidebar_label: 'Buffer'
sidebar_position: 120
slug: /engines/table-engines/special/buffer
title: 'Buffer 表引擎'
doc_type: 'reference'
---

将数据缓存在 RAM 中，并定期将其刷新到另一张表。在读取时，会同时从缓冲区和另一张表中读取数据。

:::note
建议使用[异步插入](/zh/guides/best-practices/asyncinserts.md)作为 Buffer 表引擎的替代方案。
:::

```sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes [,flush_time [,flush_rows [,flush_bytes]]])
```

<div id="engine-parameters">
  ### 引擎参数
</div>

<div id="database">
  #### `database`
</div>

`database` — 数据库名称。您可以使用 `currentDatabase()` 或其他返回字符串的常量表达式。

<div id="table">
  #### `table`
</div>

`table` – 用于刷写数据的表。

<div id="num_layers">
  #### `num_layers`
</div>

`num_layers` – 并行层数。从物理上看，该表将表示为 `num_layers` 个相互独立的缓冲层。

<div id="min_time-max_time-min_rows-max_rows-min_bytes-and-max_bytes">
  #### `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, and `max_bytes`
</div>

将数据从缓冲区刷出时的条件。

<div id="optional-engine-parameters">
  ### 可选引擎参数
</div>

<div id="flush_time-flush_rows-and-flush_bytes">
  #### `flush_time`, `flush_rows`, and `flush_bytes`
</div>

在后台从缓冲区刷写数据的条件 (省略或为零表示不使用 `flush*` 参数) 。

如果满足所有 `min*` 条件，或至少满足一个 `max*` 条件，数据就会从缓冲区刷写并写入目标表。

此外，如果至少满足一个 `flush*` 条件，则会在后台触发一次刷写。这与 `max*` 不同，因为 `flush*` 允许你单独配置后台刷写，从而避免对 Buffer 表执行 `INSERT` 查询时增加延迟。

<div id="min_time-max_time-and-flush_time">
  #### `min_time`, `max_time`, and `flush_time`
</div>

以自首次写入缓冲区起经过的时间 (秒) 为条件。

<div id="min_rows-max_rows-and-flush_rows">
  #### `min_rows`, `max_rows`, and `flush_rows`
</div>

缓冲区中行数的限制条件。

<div id="min_bytes-max_bytes-and-flush_bytes">
  #### `min_bytes`, `max_bytes`, and `flush_bytes`
</div>

用于判断缓冲区中字节数的条件。

在写入过程中，数据会被插入到一个或多个随机选择的缓冲区中 (由 `num_layers` 配置) 。或者，如果要插入的数据分区片段足够大 (大于 `max_rows` 或 `max_bytes`) ，则会直接写入目标表，跳过缓冲区。

数据刷出的条件会针对 `num_layers` 个缓冲区中的每一个分别计算。例如，如果 `num_layers = 16` 且 `max_bytes = 100000000`，则最大 RAM 占用为 1.6 GB。

示例：

```sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 1, 10, 100, 10000, 1000000, 10000000, 100000000)
```

创建一个与 `merge.hits` 结构相同、使用 Buffer 引擎的 `merge.hits_buffer` 表。向该表写入时，数据会先缓存在 RAM 中，随后再写入 &#39;merge.hits&#39; 表。系统会创建一个缓冲区，并在满足以下任一条件时将数据刷写：

* 自上次刷写以来已过去 100 秒 (`max_time`) 或
* 已写入 100 万行 (`max_rows`) 或
* 已写入 100 MB 数据 (`max_bytes`) 或
* 已过去 10 秒 (`min_time`) ，且已写入 10,000 行 (`min_rows`) 和 10 MB (`min_bytes`) 数据

例如，如果只写入了一行数据，那么无论如何，100 秒后它都会被刷写。但如果写入了很多行，数据就会更早刷写。

当服务器停止时，或执行 `DROP TABLE` 或 `DETACH TABLE` 时，缓冲的数据也会刷写到目标表。

你可以将数据库和表名设置为空字符串 (用单引号括起来) 。这表示不存在目标表。在这种情况下，达到数据刷写条件时，缓冲区会被直接清空。这对于在内存中保留一段时间窗口内的数据可能很有用。

从 Buffer 表读取时，数据会同时从缓冲区和目标表 (如果存在) 中读取。
请注意，Buffer 表不支持索引。换句话说，缓冲区中的数据会被全表扫描，在缓冲区很大时可能会很慢。 (对于下层表中的数据，则会使用其支持的索引。)

如果 Buffer 表中的列集合与下层表中的列集合不一致，则会插入两个表中共同存在的列子集。

如果 Buffer 表中的某一列与下层表中对应列的类型不匹配，服务器日志中会记录一条错误消息，并且缓冲区会被清空。
如果缓冲区刷写时下层表不存在，也会发生同样的情况。

:::note
在 2021 年 10 月 26 日之前发布的发行版中，对 Buffer 表执行 ALTER 会导致 `Block structure mismatch` 错误 (参见 [#15117](https://github.com/ClickHouse/ClickHouse/issues/15117) 和 [#30565](https://github.com/ClickHouse/ClickHouse/pull/30565)) ，因此唯一的办法是删除 Buffer 表后重新创建。在尝试对 Buffer 表执行 ALTER 之前，请先确认你的发行版中此错误已被修复。
:::

如果服务器异常重启，缓冲区中的数据会丢失。

`FINAL` 和 `SAMPLE` 对 Buffer 表无法正常工作。这些条件会传递给目标表，但不会用于处理缓冲区中的数据。如果需要这些功能，我们建议只将 Buffer 表用于写入，而从目标表读取。

向 Buffer 表写入数据时，其中一个缓冲区会被锁定。如果同时还在从该表读取数据，就会产生延迟。

插入到 Buffer 表中的数据，最终写入下层表时，顺序和块都可能发生变化。因此，很难使用 Buffer 表正确地向 CollapsingMergeTree 写入数据。为避免问题，你可以将 `num_layers` 设置为 1。

如果目标表是 replicated 的，那么通过 Buffer 表写入时，复制表的一些预期特性会丢失。行顺序和数据 parts 大小的随机变化会导致数据去重失效，这意味着无法对复制表实现可靠的 &#39;exactly once&#39; 写入。

由于这些缺点，我们只建议在极少数情况下使用 Buffer 表。

当单位时间内从大量服务器接收到过多 INSERT，且数据无法在插入前进行缓冲，也就是说 INSERT 的执行速度不够快时，可以使用 Buffer 表。

请注意，即使对于 Buffer 表，逐行插入数据也没有意义。这样速度每秒只能达到几千行，而插入更大的数据块时，速度每秒可超过一百万行。