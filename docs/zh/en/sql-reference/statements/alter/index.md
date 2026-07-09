---
description: 'ALTER 文档'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
doc_type: 'reference'
---

大多数 `ALTER TABLE` 查询用于修改表设置或数据：

| Modifier                                                                    |
| --------------------------------------------------------------------------- |
| [COLUMN](/zh/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/zh/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/zh/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/zh/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/zh/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/zh/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/zh/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/zh/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/zh/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/zh/sql-reference/statements/alter/apply-deleted-mask.md) |
| [APPLY PATCHES](/zh/sql-reference/statements/alter/apply-patches.md)           |

:::note
大多数 `ALTER TABLE` 查询仅支持 [*MergeTree](/zh/engines/table-engines/mergetree-family/index.md)、[Merge](/zh/engines/table-engines/special/merge.md) 和 [Distributed](/zh/engines/table-engines/special/distributed.md) 表。
:::

这些 `ALTER` 语句用于操作视图：

| Statement                                                               | Description                                                        |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------ |
| [ALTER TABLE ... MODIFY QUERY](/zh/sql-reference/statements/alter/view.md) | 修改 [materialized view](/zh/sql-reference/statements/create/view) 的结构。 |

这些 `ALTER` 语句用于修改与基于角色的访问控制相关的实体：

| Statement                                                               |
| ----------------------------------------------------------------------- |
| [USER](/zh/sql-reference/statements/alter/user.md)                         |
| [ROLE](/zh/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/zh/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/zh/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/zh/sql-reference/statements/alter/settings-profile.md) |

| Statement                                                                     | Description                                               |
| ----------------------------------------------------------------------------- | --------------------------------------------------------- |
| [ALTER TABLE ... MODIFY COMMENT](/zh/sql-reference/statements/alter/comment.md)  | 为表添加、修改或删除注释，无论之前是否已设置。                                   |
| [ALTER NAMED COLLECTION](/zh/sql-reference/statements/alter/named-collection.md) | 修改 [Named Collections](/zh/operations/named-collections.md)。 |

<div id="mutations">
  ## 变更
</div>

用于修改表数据的 `ALTER` 查询是通过一种称为“mutations”的机制实现的，最典型的是 [ALTER TABLE ... DELETE](/zh/sql-reference/statements/alter/delete.md) 和 [ALTER TABLE ... UPDATE](/zh/sql-reference/statements/alter/update.md)。它们是异步后台进程，类似于 [MergeTree](/zh/engines/table-engines/mergetree-family/index.md) 表中的合并操作，用于生成新的“mutated”数据分区片段版本。

对于 `*MergeTree` 表，变更通过**重写整个数据分区片段**来执行。
它不具备原子性——数据分区片段一旦准备就绪，就会立即被变更后的数据分区片段替换；而在变更执行期间开始的 `SELECT` 查询，会同时看到已变更的数据分区片段中的数据和尚未变更的数据分区片段中的数据。

变更会按其创建顺序进行全序排列，并按该顺序应用到每个数据分区片段上。变更与 `INSERT INTO` 查询之间也存在部分顺序关系：在变更提交之前插入到表中的数据会被变更，而之后插入的数据则不会被变更。请注意，变更不会以任何方式阻塞插入。

变更查询在添加完变更条目后会立即返回 (对于复制表，是写入 ZooKeeper；对于非复制表，则是写入文件系统) 。变更本身会使用系统 profile 设置异步执行。要跟踪变更进度，可以使用 [`system.mutations`](/zh/operations/system-tables/mutations) 表。已成功提交的变更即使在 ClickHouse 服务器重启后也会继续执行。变更一旦提交就无法回滚，但如果由于某种原因卡住了，可以使用 [`KILL MUTATION`](/zh/sql-reference/statements/kill.md/#kill-mutation) 查询将其取消。

已完成变更的条目不会立即删除 (保留条目的数量由 `finished_mutations_to_keep` 存储引擎参数决定) 。较早的变更条目会被删除。

<div id="synchronicity-of-alter-queries">
  ## `ALTER` 查询的同步性
</div>

对于非复制表，所有 `ALTER` 查询都会同步执行。对于复制表，查询只会将相应操作的指令添加到 `ZooKeeper`，而这些操作本身会尽快执行。不过，查询也可以等待，直到所有副本都完成这些操作。

对于会创建变更 (mutation) 的 `ALTER` 查询 (例如但不限于 `UPDATE`、`DELETE`、`MATERIALIZE INDEX`、`MATERIALIZE PROJECTION`、`MATERIALIZE COLUMN`、`APPLY DELETED MASK`、`APPLY PATCHES`、`CLEAR STATISTIC`、`MATERIALIZE STATISTIC`) ，其同步性由 [mutations&#95;sync](/zh/operations/settings/settings.md/#mutations_sync) 设置决定。

对于其他仅修改元数据的 `ALTER` 查询，可以使用 [alter&#95;sync](/zh/operations/settings/settings#alter_sync) 设置来配置等待行为。

你可以使用 [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/zh/operations/settings/settings#replication_wait_for_inactive_replica_timeout) 设置，指定等待非活动副本执行完所有 `ALTER` 查询的时长 (以秒为单位) 。

:::note
对于所有 `ALTER` 查询，如果 `alter_sync = 2`，且某些副本处于非活动状态的时间超过 `replication_wait_for_inactive_replica_timeout` 设置中指定的时长，则会抛出 `UNFINISHED` 异常。
:::

<div id="related-content">
  ## 相关内容
</div>

* 博客：[ClickHouse 中的更新与删除处理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)