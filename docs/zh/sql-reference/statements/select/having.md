---
sidebar_label: HAVING
---

# HAVING 子句 {#having-clause}

允许过滤由 [GROUP BY](../../../sql-reference/statements/select/group-by.md) 生成的聚合结果. 它类似于 [WHERE](../../../sql-reference/statements/select/where.md) ，但不同的是 `WHERE` 在聚合之前执行，而 `HAVING` 之后进行。

可以从 `SELECT` 生成的聚合结果中通过他们的别名来执行 `HAVING` 子句。 或者 `HAVING` 子句可以筛选查询结果中未返回的其他聚合的结果。

## 限制 {#limitations}

`HAVING` 如果不执行聚合则无法使用。 使用 `WHERE` 则相反。
