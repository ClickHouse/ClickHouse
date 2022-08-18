---
toc_priority: 6
---

# any {#agg_function-any}

选择第一个遇到的值。
查询可以以任何顺序执行，甚至每次都以不同的顺序执行，因此此函数的结果是不确定的。
要获得确定的结果，您可以使用 ‘min’ 或 ‘max’ 功能，而不是 ‘any’.

在某些情况下，可以依靠执行的顺序。 这适用于SELECT来自使用ORDER BY的子查询的情况。

当一个 `SELECT` 查询具有 `GROUP BY` 子句或至少一个聚合函数，ClickHouse（相对于MySQL）要求在所有表达式 `SELECT`, `HAVING`，和 `ORDER BY` 子句可以从键或聚合函数计算。 换句话说，从表中选择的每个列必须在键或聚合函数内使用。 要获得像MySQL这样的行为，您可以将其他列放在 `any` 聚合函数。
