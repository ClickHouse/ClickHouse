---
description: '轻量级更新通过补丁分区片段简化了数据库中数据更新的过程。'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: '轻量级 UPDATE 语句'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

:::note
轻量级更新目前仍处于 Beta 阶段。
如果你遇到问题，请在 [ClickHouse 仓库](https://github.com/clickhouse/clickhouse/issues) 中提交 issue。
:::

轻量级 `UPDATE` 语句会更新表 `[db.]table` 中与表达式 `filter_expr` 匹配的行。
之所以称为“轻量级更新”，是为了与 [`ALTER TABLE ... UPDATE`](/zh/sql-reference/statements/alter/update) 查询相区别；后者是一个重量级过程，会重写数据分区片段中的整列。
它仅适用于 [`MergeTree`](/zh/engines/table-engines/mergetree-family/mergetree) 表引擎家族。

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

`filter_expr` 的类型必须为 `UInt8`。此查询会将指定列的值更新为相应表达式的值，适用于 `filter_expr` 取值非零的那些行。
这些值会通过 `CAST` 运算符转换为相应的列类型。不支持更新用于计算主键或分区键的列。

<div id="examples">
  ## 示例
</div>

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

<div id="lightweight-update-does-not-update-data-immediately">
  ## 轻量级更新不会立即更新数据
</div>

轻量级 `UPDATE` 是通过 **补丁分区片段** 实现的——这是一种特殊的数据分区片段，只包含已更新的列和行。
轻量级 `UPDATE` 会创建补丁分区片段，但不会立即在存储中从物理层面修改原始数据。
更新过程类似于 `INSERT ... SELECT ...` 查询，但 `UPDATE` 查询会等到补丁分区片段创建完成后才返回。

更新后的值会：

* 通过应用补丁，在 `SELECT` 查询中**立即可见**
* 仅在后续的 合并 和 变更 过程中**物理 materialized**
* 一旦所有活跃 parts 都已将补丁 materialized，便会被**自动清理**

<div id="lightweight-update-requirements">
  ## 轻量级更新要求
</div>

[`MergeTree`](/zh/engines/table-engines/mergetree-family/mergetree)、[`ReplacingMergeTree`](/zh/engines/table-engines/mergetree-family/replacingmergetree)、[`CollapsingMergeTree`](/zh/engines/table-engines/mergetree-family/collapsingmergetree)、[`VersionedCollapsingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/versionedcollapsingmergetree) 引擎及其 [`Replicated`](/zh/engines/table-engines/mergetree-family/replication.md) 和 [`Shared`](/zh/cloud/reference/shared-merge-tree) 版本均支持轻量级更新。

要使用轻量级更新，必须通过表设置 [`enable_block_number_column`](/zh/operations/settings/merge-tree-settings#enable_block_number_column) 和 [`enable_block_offset_column`](/zh/operations/settings/merge-tree-settings#enable_block_offset_column) 启用 `_block_number` 和 `_block_offset` 列的物化。

<div id="lightweight-delete">
  ## 轻量级删除
</div>

[轻量级 `DELETE`](/zh/sql-reference/statements/delete) 查询可以作为轻量级 `UPDATE` 执行，而不是以 `ALTER UPDATE` 变更的方式执行。轻量级 `DELETE` 的实现受设置 [`lightweight_delete_mode`](/zh/operations/settings/settings#lightweight_delete_mode) 控制。

<div id="performance-considerations">
  ## 性能注意事项
</div>

**轻量级更新的优势：**

* 更新延迟与 `INSERT ... SELECT ...` 查询的延迟相当
* 只会写入已更新的列和值，而不是将数据分区片段中的整列重写
* 无需等待当前正在运行的合并/变更完成，因此更新延迟是可预期的
* 轻量级更新支持并行执行

**潜在的性能影响：**

* 会给需要应用补丁的 `SELECT` 查询带来额外开销
* 对于存在待应用补丁的数据分区片段中的列，[跳过索引](/zh/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes)将不会生效。如果表中存在补丁分区片段，则[投影](/zh/engines/table-engines/mergetree-family/mergetree.md/#projections)也不会被使用，包括那些没有待应用补丁的数据分区片段
* 过于频繁的小规模更新可能会导致“parts 过多”错误。建议将多次更新合并到一个查询中，例如把要更新的 id 放到 `WHERE` 子句中的同一个 `IN` 子句里
* 轻量级更新适合更新少量行 (最多约占表的 10%) 。如果需要更新更多数据，建议使用 [`ALTER TABLE ... UPDATE`](/zh/sql-reference/statements/alter/update) 变更

<div id="concurrent-operations">
  ## 并发操作
</div>

与重型变更不同，轻量级更新不会等待当前正在进行的合并/变更完成。
并发轻量级更新的一致性由设置 [`update_sequential_consistency`](/zh/operations/settings/settings#update_sequential_consistency) 和 [`update_parallel_mode`](/zh/operations/settings/settings#update_parallel_mode) 控制。

<div id="update-permissions">
  ## 更新权限
</div>

`UPDATE` 需要 `ALTER UPDATE` 权限。要允许指定用户在特定表上执行 `UPDATE` 语句，请运行：

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

<div id="details-of-the-implementation">
  ## 实现细节
</div>

补丁分区片段与常规 parts 相同，但只包含已更新的列以及若干系统列：

* `_part` - 原始 part 的名称
* `_part_offset` - 原始 part 中的行号
* `_block_number` - 原始 part 中该行的块编号
* `_block_offset` - 原始 part 中该行的块偏移量
* `_data_version` - 更新后数据的数据版本 (为 `UPDATE` 查询分配的块编号)

平均下来，补丁分区片段中每个已更新行会额外增加约 40 字节的开销 (未压缩数据) 。
系统列有助于定位原始 part 中需要更新的行。
系统列与原始 part 中的[虚拟列](/zh/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns)相关；如果需要应用补丁分区片段，这些列会在读取时添加。
补丁分区片段按 `_part` 和 `_part_offset` 排序。

补丁分区片段所在的分区与原始 part 不同。
补丁分区片段的分区 ID 为 `patch-<hash of column names in patch part>-<original_partition_id>`。
因此，包含不同列的补丁分区片段会存储在不同分区中。
例如，三次更新 `SET x = 1 WHERE <cond>`、`SET y = 1 WHERE <cond>` 和 `SET x = 1, y = 1 WHERE <cond>` 会在三个不同的分区中创建三个补丁分区片段。

补丁分区片段之间可以相互合并，以减少 `SELECT` 查询中需要应用的补丁数量并降低开销。补丁分区片段的合并使用 [ReplacingMergeTree](/zh/engines/table-engines/mergetree-family/replacingmergetree) 的合并算法，并以 `_data_version` 作为版本列。
因此，补丁分区片段始终会为 part 中每个已更新的行保存最新版本。

轻量级更新不会等待当前正在运行的 合并 和 变更 完成，而是始终基于数据分区片段的当前快照执行更新并生成补丁分区片段。
因此，应用补丁分区片段时可能会出现两种情况。

例如，如果我们读取 part `A`，则需要应用补丁分区片段 `X`：

* 如果 `X` 包含 part `A` 本身。这种情况发生在执行 `UPDATE` 时，`A` 没有参与合并。
* 如果 `X` 包含 part `B` 和 `C`，而它们已被 part `A` 覆盖。这种情况发生在执行 `UPDATE` 时，合并 (`B`、`C`) -&gt; `A` 正在运行。

对于这两种情况，分别有两种应用补丁分区片段的方式：

* 按已排序列 `_part`、`_part_offset` 进行 merge。
* 通过 `_block_number`、`_block_offset` 列进行 join。

join 模式比 merge 模式更慢，而且需要更多内存，但使用得较少。

<div id="related-content">
  ## 相关内容
</div>

* [`ALTER UPDATE`](/zh/sql-reference/statements/alter/update) - 高开销的 `UPDATE` 操作
* [Lightweight `DELETE`](/zh/sql-reference/statements/delete) - 轻量级 `DELETE` 操作
* [`APPLY PATCHES`](/zh/sql-reference/statements/alter/apply-patches) - 强制将补丁物理物化到数据分区片段中 (变更操作)