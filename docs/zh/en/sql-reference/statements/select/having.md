---
description: 'HAVING 子句文档'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'HAVING 子句'
doc_type: 'reference'
---

可用于过滤由 [GROUP BY](/zh/sql-reference/statements/select/group-by) 产生的聚合结果。它与 [WHERE](../../../sql-reference/statements/select/where.md) 子句类似，区别在于：`WHERE` 在聚合前执行，而 `HAVING` 在聚合后执行。

在 `HAVING` 子句中，可以通过别名引用 `SELECT` 子句中的聚合结果。`HAVING` 子句也可以对未包含在查询结果中的额外聚合结果进行过滤。

<div id="example">
  ## 示例
</div>

如果你有一个如下所示的 `sales` 表：

```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

你可以这样查询它：

```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```

这将列出所在区域总销售额超过 10,000 的销售人员。

<div id="limitations">
  ## 限制
</div>

未执行聚合时，不能使用 `HAVING`。请改用 `WHERE`。