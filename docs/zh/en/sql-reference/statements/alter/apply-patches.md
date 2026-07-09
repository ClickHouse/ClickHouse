---
description: '有关应用轻量级更新中的补丁的文档'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: '应用轻量级更新中的补丁'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

该命令会手动触发由 [lightweight `UPDATE`](/zh/sql-reference/statements/update) 语句创建的补丁分区片段的物理物化。它会仅重写受影响的列，强制将待处理补丁应用到数据分区片段。

:::note

* 它仅适用于 [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) 家族中的表 (包括[复制](../../../engines/table-engines/mergetree-family/replication.md)表) 。
* 这是一个变更操作，会在后台异步执行。
  :::

<div id="when-to-use">
  ## 何时使用 APPLY PATCHES
</div>

:::tip
通常不需要使用 `APPLY PATCHES`
:::

启用 [`apply_patches_on_merge`](/zh/operations/settings/merge-tree-settings#apply_patches_on_merge) 设置后 (默认如此) ，补丁分区片段通常会在合并过程中自动应用。不过，在以下场景中，你可能会希望手动触发补丁应用：

* 减少在 `SELECT` 查询期间应用补丁带来的开销
* 在多个补丁分区片段积累起来之前先将其整合
* 为备份或导出准备数据，并使补丁已 materialized
* 当 `apply_patches_on_merge` 被禁用，而你希望自行控制补丁的应用时机时

<div id="examples">
  ## 示例
</div>

对某个表应用所有待处理补丁：

```sql
ALTER TABLE my_table APPLY PATCHES;
```

仅对特定分区应用补丁：

```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

与其他操作结合使用：

```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

<div id="monitor">
  ## 监控补丁应用进度
</div>

您可以使用 [`system.mutations`](/zh/operations/system-tables/mutations) 表来监控补丁应用的进度：

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

<div id="see-also">
  ## 另见
</div>

* [轻量级 `UPDATE`](/zh/sql-reference/statements/update) - 通过轻量级更新创建补丁分区片段
* [`apply_patches_on_merge` 设置](/zh/operations/settings/merge-tree-settings#apply_patches_on_merge) - 控制在合并过程中自动应用补丁