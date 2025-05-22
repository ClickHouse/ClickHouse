---
slug: /zh/sql-reference/statements/alter/overview
sidebar_position: 35
sidebar_label: ALTER
---

## ALTER {#query_language_queries_alter}

大多数 `ALTER TABLE` 查询修改表设置或数据:

-   [COLUMN](../../../sql-reference/statements/alter/column.mdx)
-   [PARTITION](../../../sql-reference/statements/alter/partition.mdx)
-   [DELETE](../../../sql-reference/statements/alter/delete.md)
-   [UPDATE](../../../sql-reference/statements/alter/update.md)
-   [ORDER BY](../../../sql-reference/statements/alter/order-by.md)
-   [INDEX](../../../sql-reference/statements/alter/index/index.md)
-   [CONSTRAINT](../../../sql-reference/statements/alter/constraint.md)
-   [TTL](../../../sql-reference/statements/alter/ttl.md)

:::note
大多数 `ALTER TABLE` 查询只支持[\*MergeTree](../../../engines/table-engines/mergetree-family/index.md)表，以及[Merge](../../../engines/table-engines/special/merge.md)和[Distributed](../../../engines/table-engines/special/distributed.md)。
:::

这些 `ALTER` 语句操作视图:

-   [ALTER TABLE ... MODIFY QUERY](../../../sql-reference/statements/alter/view.md) — 修改一个 [Materialized view](../create/view.md#materialized) 结构.
-   [ALTER LIVE VIEW](../../../sql-reference/statements/alter/view.md#alter-live-view) — 刷新一个 [Live view](../create/view.md#live-view).

这些 `ALTER` 语句修改与基于角色的访问控制相关的实体:

-   [USER](../../../sql-reference/statements/alter/user.mdx)
-   [ROLE](../../../sql-reference/statements/alter/role.md)
-   [QUOTA](../../../sql-reference/statements/alter/quota.mdx)
-   [ROW POLICY](../../../sql-reference/statements/alter/row-policy.md)
-   [SETTINGS PROFILE](../../../sql-reference/statements/alter/settings-profile.md)

[ALTER TABLE ... MODIFY COMMENT](../alter/index.md) 语句添加、修改或删除表中的注释，无论之前是否设置过。

## Mutations 突变 {#mutations}

用来操作表数据的ALTER查询是通过一种叫做“突变”的机制来实现的，最明显的是[ALTER TABLE ... DELETE](../../../sql-reference/statements/alter/delete.md)和[ALTER TABLE ... UPDATE](../../../sql-reference/statements/alter/update.md)。它们是异步的后台进程，类似于[MergeTree](../../../engines/table-engines/mergetree-family/index.md)表的合并，产生新的“突变”版本的部件。



对于 `*MergeTree` 表，通过重写整个数据部分来执行突变。没有原子性——一旦突变的部件准备好，部件就会被替换，并且在突变期间开始执行的 `SELECT` 查询将看到来自已经突变的部件的数据，以及来自尚未突变的部件的数据。



突变完全按照它们的产生顺序排列，并按此顺序应用于每个部分。突变还与“INSERT INTO”查询进行部分排序:在提交突变之前插入表中的数据将被突变，而在此之后插入的数据将不会被突变。注意，突变不会以任何方式阻止插入。



突变查询在添加突变条目后立即返回(对于复制表到ZooKeeper，对于非复制表到文件系统)。突变本身使用系统配置文件设置异步执行。要跟踪突变的进程，可以使用[`system.mutations`](../../../operations/system-tables/mutations.md#system_tables-mutations) 表。成功提交的变异将继续执行，即使ClickHouse服务器重新启动。没有办法回滚突变一旦提交，但如果突变卡住了，它可以取消与[`KILL MUTATION`](../../../sql-reference/statements/misc.md#kill-mutation) 查询。



完成突变的条目不会立即删除(保留条目的数量由 `finished_mutations_to_keep` 存储引擎参数决定)。删除旧的突变条目。

## ALTER 查询的同步性 {#synchronicity-of-alter-queries}


对于非复制表，所有的 `ALTER` 查询都是同步执行的。对于复制表，查询只是向“ZooKeeper”添加相应动作的指令，动作本身会尽快执行。但是，查询可以等待所有副本上的这些操作完成。

对于所有的“ALTER”查询，您可以使用[alter_sync](../../../operations/settings/settings.md#alter-sync)设置等待。

通过[replication_wait_for_inactive_replica_timeout](../../../operations/settings/settings.md#replication-wait-for-inactive-replica-timeout]设置，可以指定不活动的副本执行所有 `ALTER` 查询的等待时间(以秒为单位)。



!!! info "备注"

    对于所有的 `ALTER` 查询，如果  `alter_sync = 2`  和一些副本的不激活时间超过时间(在 `replication_wait_for_inactive_replica_timeout` 设置中指定)，那么将抛出一个异常 `UNFINISHED`。



对于 `ALTER TABLE ... UPDATE|DELETE` 查询由 [mutations_sync](../../../operations/settings/settings.md#mutations_sync) 设置定义的同步度。
