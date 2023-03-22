---
sidebar_label: FROM
---

# FROM子句 {#select-from}

`FROM` 子句指定从以下数据源中读取数据:

-   [表](../../../engines/table-engines/index.md)
-   [子查询](../../../sql-reference/statements/select/index.md) {## TODO: better link ##}
-   [表函数](../../../sql-reference/table-functions/index.md#table-functions)

[JOIN](../../../sql-reference/statements/select/join.md) 和 [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) 子句也可以用来扩展 `FROM` 的功能

子查询是另一个 `SELECT` 可以指定在 `FROM` 后的括号内的查询。

`FROM` 子句可以包含多个数据源，用逗号分隔，这相当于在他们身上执行 [CROSS JOIN](../../../sql-reference/statements/select/join.md)

## FINAL 修饰符 {#select-from-final}

当 `FINAL` 被指定，ClickHouse会在返回结果之前完全合并数据，从而执行给定表引擎合并期间发生的所有数据转换。

它适用于从使用 [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)-引擎族. 还支持:

-   [Replicated](../../../engines/table-engines/mergetree-family/replication.md) 版本 `MergeTree` 引擎
-   [View](../../../engines/table-engines/special/view.md), [Buffer](../../../engines/table-engines/special/buffer.md), [Distributed](../../../engines/table-engines/special/distributed.md)，和 [MaterializedView](../../../engines/table-engines/special/materializedview.md) 在其他引擎上运行的引擎，只要是它们底层是 `MergeTree`-引擎表即可。

现在使用 `FINAL` 修饰符 的 `SELECT` 查询启用了并发执行, 这会快一点。但是仍然存在缺陷 (见下)。  [max_final_threads](../../../operations/settings/settings.md#max-final-threads) 设置使用的最大线程数限制。

### 缺点 {#drawbacks}

使用的查询 `FINAL` 执行速度比类似的查询慢一点，因为:

-   在查询执行期间合并数据。
-   查询与 `FINAL` 除了读取查询中指定的列之外，还读取主键列。

**在大多数情况下，避免使用 `FINAL`.** 常见的方法是使用假设后台进程的不同查询 `MergeTree` 引擎还没有发生，并通过应用聚合（例如，丢弃重复项）来处理它。 {## TODO: examples ##}

## 实现细节 {#implementation-details}

如果 `FROM` 子句被省略，数据将从读取 `system.one` 表。
该 `system.one` 表只包含一行（此表满足与其他 DBMS 中的 DUAL 表有相同的作用）。

若要执行查询，将从相应的表中提取查询中列出的所有列。 外部查询不需要的任何列都将从子查询中抛出。
如果查询未列出任何列（例如, `SELECT count() FROM t`），无论如何都会从表中提取一些列（首选是最小的列），以便计算行数。
