---
slug: /zh/sql-reference/statements/alter/index
toc_hidden_folder: true
sidebar_position: 42
sidebar_label: INDEX
---

# 操作数据跳过索引 {#manipulations-with-data-skipping-indices}

可以使用以下操作：

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value [FIRST|AFTER name]` - 向表元数据添加索引描述。

-   `ALTER TABLE [db].name DROP INDEX name` - 从表元数据中删除索引描述并从磁盘中删除索引文件。

-   `ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name` - 查询在分区`partition_name`中重建二级索引`name`。 操作为[mutation](../index.md#mutations).

前两个命令是轻量级的，它们只更改元数据或删除文件。

Also, they are replicated, syncing indices metadata via ZooKeeper.
此外，它们会被复制，会通过ZooKeeper同步索引元数据。

:::note "注意"
索引操作仅支持具有以下特征的表 [`*MergeTree`](../../../../engines/table-engines/mergetree-family/mergetree.md)引擎 (包括[replicated](../../../../engines/table-engines/mergetree-family/replication.md)).
:::
