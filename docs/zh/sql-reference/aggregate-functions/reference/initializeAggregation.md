---
toc_priority: 150
---

## initializeAggregation {#initializeaggregation}

初始化你输入行的聚合。用于后缀是 `State` 的函数。
用它来测试或处理 `AggregateFunction` 和 `AggregationgMergeTree` 类型的列。

**语法**

``` sql
initializeAggregation (aggregate_function, column_1, column_2)
```

**参数**

-   `aggregate_function` — 聚合函数名。 这个函数的状态 — 正创建的。[String](../../../sql-reference/data-types/string.md#string)。
-   `column_n` — 将其转换为函数的参数的列。[String](../../../sql-reference/data-types/string.md#string)。

**返回值**

返回输入行的聚合结果。返回类型将与 `initializeAgregation` 用作第一个参数的函数的返回类型相同。
例如，对于后缀为 `State` 的函数，返回类型将是 `AggregateFunction`。

**示例**

查询:

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM system.numbers LIMIT 10000);
```
结果:

┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘
