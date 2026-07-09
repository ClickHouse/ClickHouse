---
description: 'DISTINCT 子句文档'
sidebar_label: 'DISTINCT'
slug: /sql-reference/statements/select/distinct
title: 'DISTINCT 子句'
doc_type: 'reference'
---

如果指定 `SELECT DISTINCT`，查询结果中将只保留唯一的行。也就是说，对于结果中所有完全相同的行，每组最终只保留一行。

你可以指定哪些列的值必须唯一：`SELECT DISTINCT ON (column1, column2,...)`。如果未指定列，则会考虑所有列。

考虑下列表：

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

不指定列时使用 `DISTINCT`：

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

对指定列使用 `DISTINCT`：

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

<div id="distinct-and-order-by">
  ## DISTINCT 和 ORDER BY
</div>

ClickHouse 支持在同一查询中对不同列使用 `DISTINCT` 和 `ORDER BY` 子句。`DISTINCT` 子句会先于 `ORDER BY` 子句执行。

假设有如下表：

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

查询数据：

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

按不同排序方向选择数据：

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

行 `2, 4` 在排序前已被截断。

编写查询时，请将这一实现细节考虑在内。

<div id="null-processing">
  ## NULL 处理
</div>

`DISTINCT` 对 [NULL](/zh/sql-reference/syntax#null) 的处理方式就好像 `NULL` 是一个特定的值，并且 `NULL==NULL`。换句话说，在 `DISTINCT` 的结果中，包含 `NULL` 的不同组合只会出现一次。这与大多数其他上下文中对 `NULL` 的处理方式有所不同。

<div id="alternatives">
  ## 替代方案
</div>

也可以不使用任何聚合函数，而是对 `SELECT` 子句中指定的同一组值使用 [GROUP BY](/zh/sql-reference/statements/select/group-by)，从而得到相同的结果。不过，与 `GROUP BY` 这种方式相比，还是有一些区别：

* `DISTINCT` 可以与 `GROUP BY` 同时使用。
* 如果省略 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 且指定了 [LIMIT](../../../sql-reference/statements/select/limit.md)，查询在读取到所需数量的不同行后会立即停止。
* 数据块会在处理过程中直接输出，无需等到整个查询运行结束。