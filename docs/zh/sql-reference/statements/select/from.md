---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: FROM
---

# FROM条款 {#select-from}

该 `FROM` 子句指定从中读取数据的源:

-   [表](../../../engines/table-engines/index.md)
-   [子查询](../../../sql-reference/statements/select/index.md) {## TODO: better link ##}
-   [表函数](../../../sql-reference/table-functions/index.md#table-functions)

[JOIN](../../../sql-reference/statements/select/join.md) 和 [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) 子句也可以用来扩展的功能 `FROM` 条款

子查询是另一个 `SELECT` 可以在括号内指定的查询 `FROM` 条款

`FROM` 子句可以包含多个数据源，用逗号分隔，这相当于执行 [CROSS JOIN](../../../sql-reference/statements/select/join.md) 在他们身上

## 最终修饰符 {#select-from-final}

当 `FINAL` 如果指定，ClickHouse会在返回结果之前完全合并数据，从而执行给定表引擎合并期间发生的所有数据转换。

它适用于从使用 [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)-发动机系列（除了 `GraphiteMergeTree`). 还支持:

-   [复制](../../../engines/table-engines/mergetree-family/replication.md) 版本 `MergeTree` 引擎
-   [查看](../../../engines/table-engines/special/view.md), [缓冲区](../../../engines/table-engines/special/buffer.md), [分布](../../../engines/table-engines/special/distributed.md)，和 [MaterializedView](../../../engines/table-engines/special/materializedview.md) 在其他引擎上运行的引擎，只要它们是在创建 `MergeTree`-发动机表。

### 缺点 {#drawbacks}

使用的查询 `FINAL` 执行速度不如类似的查询那么快，因为:

-   查询在单个线程中执行，并在查询执行期间合并数据。
-   查询与 `FINAL` 除了读取查询中指定的列之外，还读取主键列。

**在大多数情况下，避免使用 `FINAL`.** 常见的方法是使用假设后台进程的不同查询 `MergeTree` 引擎还没有发生，并通过应用聚合（例如，丢弃重复项）来处理它。 {## TODO: examples ##}

## 实施细节 {#implementation-details}

如果 `FROM` 子句被省略，数据将从读取 `system.one` 桌子
该 `system.one` 表只包含一行（此表满足与其他Dbms中找到的双表相同的目的）。

若要执行查询，将从相应的表中提取查询中列出的所有列。 外部查询不需要的任何列都将从子查询中抛出。
如果查询未列出任何列（例如, `SELECT count() FROM t`），无论如何都会从表中提取一些列（最小的列是首选），以便计算行数。
