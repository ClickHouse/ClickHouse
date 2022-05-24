---
sidebar_label: UNION ALL
---

# UNION ALL子句 {#union-clause}

你可以使用 `UNION ALL` 结合任意数量的 `SELECT` 来扩展其结果。 示例:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

结果列通过它们的索引进行匹配（在内部的顺序 `SELECT`). 如果列名称不匹配，则从第一个查询中获取最终结果的名称。

对联合执行类型转换。 例如，如果合并的两个查询具有相同的字段与非-`Nullable` 和 `Nullable` 从兼容类型的类型，由此产生的 `UNION ALL` 有一个 `Nullable` 类型字段。

属于以下部分的查询 `UNION ALL` 不能用圆括号括起来。 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 和 [LIMIT](../../../sql-reference/statements/select/limit.md) 应用于单独的查询，而不是最终结果。 如果您需要将转换应用于最终结果，则可以将所有查询 `UNION ALL` 在子查询中 [FROM](../../../sql-reference/statements/select/from.md) 子句。

## 限制 {#limitations}

只有 `UNION ALL` 支持。 `UNION` (`UNION DISTINCT`）不支持。 如果你需要 `UNION DISTINCT`，你可以写 `SELECT DISTINCT` 子查询中包含 `UNION ALL`.

## 实现细节 {#implementation-details}

属于 `UNION ALL` 的查询可以同时运行，并且它们的结果可以混合在一起。
