---
description: '关于操作列统计信息的文档'
sidebar_label: 'STATISTICS'
sidebar_position: 45
slug: /sql-reference/statements/alter/statistics
title: '操作列统计信息'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="manipulating-column-statistics">
  # 管理列统计信息
</div>

<CloudNotSupportedBadge />

支持以下操作：

* `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - 将统计信息描述添加到表的元数据中。

* `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - 修改表元数据中的统计信息描述。

* `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - 从指定列的元数据中移除统计信息，并删除这些列在所有 parts 中的全部统计信息对象。

* `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - 删除指定列在所有 parts 中的全部统计信息对象。可使用 `ALTER TABLE MATERIALIZE STATISTICS` 重新构建这些统计信息对象。

* `ALTER TABLE [db.]table MATERIALIZE STATISTICS (ALL | [IF EXISTS] (column list))` - 重新构建列统计信息。该操作通过[变更](../../../sql-reference/statements/alter/index.md#mutations)实现。

前两条命令是轻量级的，因为它们只会修改元数据或删除文件。

此外，这些操作支持复制，并通过 ZooKeeper 同步统计信息元数据。

<div id="example">
  ## 示例：
</div>

为两列添加两种统计信息类型：

```sql
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
仅 [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) 引擎的表 (包括 [Replicated](../../../engines/table-engines/mergetree-family/replication.md) 变体) 支持统计信息。
:::