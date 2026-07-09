---
description: '始终驻留在 RAM 中的数据集。用于 `IN` 运算符的右侧。'
sidebar_label: 'Set'
sidebar_position: 60
slug: /engines/table-engines/special/set
title: 'Set 表引擎'
doc_type: 'reference'
---

:::note
在 ClickHouse Cloud 中，如果您的服务创建时使用的版本早于 25.4，则需要使用 `SET compatibility=25.4` 将兼容性至少设置为 25.4。
:::

始终驻留在 RAM 中的数据集。用于 `IN` 运算符的右侧 (请参见“IN operators”一节) 。

您可以使用 `INSERT` 向表中插入数据。新元素会添加到数据集中，重复项则会被忽略。
但您不能从该表执行 `SELECT`。获取数据的唯一方式，是将其用在 `IN` 运算符的右侧。

数据始终位于 RAM 中。执行 `INSERT` 时，插入数据的块也会写入磁盘上的表目录中。启动服务器时，这些数据会加载到 RAM 中。换句话说，重启后数据仍会保留。

如果服务器发生异常重启，磁盘上的数据块可能会丢失或损坏。在后一种情况下，您可能需要手动删除包含损坏数据的文件。

<div id="join-limitations-and-settings">
  ### 限制和设置
</div>

创建表时，会应用以下设置：

<div id="persistent">
  #### 持久化
</div>

禁用 Set 和 [Join](/zh/engines/table-engines/special/join) 表引擎的持久化。

可减少 I/O 开销。适用于追求性能且无需持久化的场景。

可选值：

* 1 — 已启用。
* 0 — 已禁用。

默认值：`1`。