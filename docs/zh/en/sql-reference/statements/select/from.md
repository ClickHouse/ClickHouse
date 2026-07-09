---
description: 'FROM 子句参考文档'
sidebar_label: 'FROM'
slug: /sql-reference/statements/select/from
title: 'FROM 子句'
doc_type: 'reference'
---

`FROM` 子句用于指定读取数据的来源：

* [表](../../../engines/table-engines/index.md)
* [子查询](../../../sql-reference/statements/select/index.md)
* [表函数](/zh/sql-reference/table-functions)

还可以使用 [JOIN](../../../sql-reference/statements/select/join.md) 和 [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) 子句来扩展 `FROM` 子句的功能。

子查询是另一个 `SELECT` 查询，可以在 `FROM` 子句中用括号括起来指定。

SQL 标准中的 `VALUES` 子句也可以用作表表达式：

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

更多详细信息，请参见 [Values 表函数](/zh/sql-reference/table-functions/values#sql-standard-values-clause)。

`FROM` 可以包含多个数据源，以逗号分隔，这等同于对它们执行 [CROSS JOIN](../../../sql-reference/statements/select/join.md)。

`FROM` 也可以选择出现在 `SELECT` 子句之前。这是对标准 SQL 的 ClickHouse 特有扩展，可使 `SELECT` 语句更易于阅读。示例：

```sql
FROM table
SELECT *
```

<div id="final-modifier">
  ## FINAL 修饰符
</div>

指定 `FINAL` 时，ClickHouse 会在返回结果前将数据完全合并。这也会执行指定表引擎在合并过程中发生的所有数据转换。

它适用于从使用以下表引擎的表中选择数据时：

* `ReplacingMergeTree`
* `SummingMergeTree`
* `AggregatingMergeTree`
* `CollapsingMergeTree`
* `VersionedCollapsingMergeTree`

带有 `FINAL` 的 `SELECT` 查询会并行执行。[max&#95;final&#95;threads](/zh/operations/settings/settings#max_final_threads) 设置会限制所使用的线程数。

<div id="drawbacks">
  ### 缺点
</div>

使用 `FINAL` 的查询执行速度会比不使用 `FINAL` 的同类查询略慢，原因如下：

* 数据会在查询执行期间进行合并。
* 使用 `FINAL` 的查询除了会读取查询中指定的列外，还可能读取主键列。

`FINAL` 需要额外的计算和内存资源，因为原本通常会在合并时发生的处理，现在必须在查询时在内存中完成。不过，为了得到准确的结果，有时必须使用 `FINAL` (因为数据可能尚未完全合并) 。与运行 `OPTIMIZE` 强制触发合并相比，其成本要低一些。

作为 `FINAL` 的替代方案，有时也可以改写查询：假设 `MergeTree` 引擎的后台处理过程尚未发生，并通过应用聚合来应对这种情况 (例如去重) 。如果你确实需要在查询中使用 `FINAL` 才能获得所需结果，也完全可以这样做，但要注意这会带来额外的处理开销。

还可以通过会话或用户 profile，使用 [FINAL](../../../operations/settings/settings.md#final) 设置将 `FINAL` 自动应用到查询中的所有表。

<div id="example-usage">
  ### 示例用法
</div>

使用 `FINAL` 关键字

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

将 `FINAL` 用作查询级别设置

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

将 `FINAL` 作为会话级设置使用

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

<div id="aliases-and-final">
  ### 别名与 FINAL
</div>

当表使用别名时，`FINAL` 要写在别名之后。这一点在 [`JOIN`](/zh/sql-reference/statements/select/join) 查询中最为明显，因为表通常都会使用别名：

```sql
SELECT t1.id, t2.name
FROM table1 AS t1 FINAL
INNER JOIN table2 AS t2 FINAL ON t1.id = t2.id;
```

`FINAL` 是作用于表引用的修饰符，因此必须放在完整的 `table [AS alias]` 表达式之后。把它放在别名之前 (`FROM table1 FINAL AS t1`) 会引发语法错误。

<div id="implementation-details">
  ## 实现细节
</div>

如果省略 `FROM` 子句，数据将从 `system.one` 表中读取。
`system.one` 表恰好只包含一行 (该表与其他数据库管理系统中的 DUAL 表作用相同) 。

执行查询时，会从相应的表中提取查询中列出的所有列。子查询中外层查询不需要的列都会被丢弃。
如果查询未列出任何列 (例如，`SELECT count() FROM t`) ，仍然会从表中提取某一列 (优先选择最小的一列) ，以便计算行数。