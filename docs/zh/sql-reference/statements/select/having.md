---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: HAVING
---

# 有条款 {#having-clause}

允许过滤由以下方式生成的聚合结果 [GROUP BY](../../../sql-reference/statements/select/group-by.md). 它类似于 [WHERE](../../../sql-reference/statements/select/where.md) 条款，但不同的是 `WHERE` 在聚合之前执行，而 `HAVING` 之后进行。

可以从以下引用聚合结果 `SELECT` 中的条款 `HAVING` 子句由他们的化名。 或者, `HAVING` 子句可以筛选查询结果中未返回的其他聚合的结果。

## 限制 {#limitations}

`HAVING` 如果不执行聚合，则无法使用。 使用 `WHERE` 相反。
