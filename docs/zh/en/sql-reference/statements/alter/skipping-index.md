---
description: '数据跳过索引操作文档'
sidebar_label: 'INDEX'
sidebar_position: 42
slug: /sql-reference/statements/alter/skipping-index
title: '管理数据跳过索引'
toc_hidden_folder: true
doc_type: 'reference'
---

支持以下操作：

<div id="add-index">
  ## ADD INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - 向表的元数据中添加索引描述。

<div id="drop-index">
  ## DROP INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - 从表元数据中移除索引描述，并删除磁盘上的索引文件。该操作以 [变更](/zh/sql-reference/statements/alter/index.md#mutations) 的形式实现。

<div id="materialize-index">
  ## MATERIALIZE INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - 为指定的 `partition_name` 重建二级索引 `name`。该操作通过 [变更](/zh/sql-reference/statements/alter/index.md#mutations) 实现。如果省略 `IN PARTITION` 部分，则会为整张表的数据重建索引。

<div id="clear-index">
  ## 清除索引
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - 从磁盘中删除二级索引文件，但不移除其描述信息。该操作通过 [变更](/zh/sql-reference/statements/alter/index.md#mutations) 实现。

命令 `ADD`、`DROP` 和 `CLEAR` 都是轻量级的，因为它们只会更改元数据或删除文件。
此外，它们还支持复制，并通过 ClickHouse Keeper 或 ZooKeeper 同步索引元数据。

:::note
只有使用 [`*MergeTree`](/zh/engines/table-engines/mergetree-family/mergetree.md) 引擎的表 (包括 [复制](/zh/engines/table-engines/mergetree-family/replication.md) 变体) 才支持索引操作。
:::