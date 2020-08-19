---
machine_translated: true
machine_translated_rev: 71d72c1f237f4a553fe91ba6c6c633e81a49e35b
---

# SimpleAggregateFunction {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction(name, types_of_arguments…)` 数据类型存储聚合函数的当前值，而不将其完整状态存储为 [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) 有 此优化可应用于具有以下属性的函数：应用函数的结果 `f` 到行集 `S1 UNION ALL S2` 可以通过应用来获得 `f` 行的部分单独设置，然后再次应用 `f` 到结果: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. 此属性保证部分聚合结果足以计算组合结果，因此我们不必存储和处理任何额外的数据。

支持以下聚合函数:

-   [`any`](../../sql-reference/aggregate-functions/reference.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference.md#agg_function-sum)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference.md#groupbitxor)
-   [`groupArrayArray`](../../sql-reference/aggregate-functions/reference.md#agg_function-grouparray)
-   [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference.md#groupuniqarrayx-groupuniqarraymax-sizex)

的值 `SimpleAggregateFunction(func, Type)` 看起来和存储方式相同 `Type`，所以你不需要应用函数 `-Merge`/`-State` 后缀。 `SimpleAggregateFunction` 具有比更好的性能 `AggregateFunction` 具有相同的聚合功能。

**参数**

-   聚合函数的名称。
-   聚合函数参数的类型。

**示例**

``` sql
CREATE TABLE t
(
    column1 SimpleAggregateFunction(sum, UInt64),
    column2 SimpleAggregateFunction(any, String)
) ENGINE = ...
```

[原始文章](https://clickhouse.tech/docs/en/data_types/simpleaggregatefunction/) <!--hide-->
