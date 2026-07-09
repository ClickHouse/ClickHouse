---
description: 'QUALIFY 子句文档'
sidebar_label: 'QUALIFY'
slug: /sql-reference/statements/select/qualify
title: 'QUALIFY 子句'
doc_type: 'reference'
---

允许对窗口函数的结果进行筛选。它与 [WHERE](../../../sql-reference/statements/select/where.md) 子句类似，不同之处在于：`WHERE` 在窗口函数求值之前执行，而 `QUALIFY` 在其之后执行。

在 `QUALIFY` 子句中，可以通过别名引用 `SELECT` 子句中的窗口函数结果。或者，`QUALIFY` 子句也可以基于未在查询结果中返回的其他窗口函数结果进行筛选。

<div id="limitations">
  ## 限制
</div>

如果没有需要计算的 窗口函数，则不能使用 `QUALIFY`。请改用 `WHERE`。

<div id="examples">
  ## 示例
</div>

示例：

```sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

```text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```