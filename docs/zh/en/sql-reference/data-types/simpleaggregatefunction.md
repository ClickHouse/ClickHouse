---
description: '关于 SimpleAggregateFunction 数据类型的文档'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'SimpleAggregateFunction 类型'
doc_type: 'reference'
---

<div id="description">
  ## 说明
</div>

`SimpleAggregateFunction` 数据类型存储的是聚合函数的中间状态，
但不像 [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md)
类型那样存储完整状态。

这种优化可用于满足以下性质的函数：

> 对行集合 `S1 UNION ALL S2` 应用函数 `f` 所得到的结果，
> 可以通过先分别对行集合的各个部分应用 `f`，再对这些结果继续应用
> `f` 来得到：`f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`。

这一性质保证了部分聚合结果就足以计算出合并后的结果，
因此无需存储和处理任何额外数据。例如，`min` 或 `max`
函数的结果无需额外步骤，就能根据中间结果计算出最终结果；而 `avg` 函数
则需要同时记录总和与计数，并在最终合并中间状态的 `Merge` 步骤中将二者相除，
从而得到平均值。

聚合函数值通常是通过调用聚合函数，并在函数名后附加
[`-SimpleState`](/zh/sql-reference/aggregate-functions/combinators#-simplestate) 组合器生成的。

<div id="syntax">
  ## 语法
</div>

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**参数**

* `aggregate_function_name` - 聚合函数名称。
* `Type` - 聚合函数参数的类型。

<div id="supported-functions">
  ## 支持的函数
</div>

支持以下聚合函数：

* [`any`](/zh/sql-reference/aggregate-functions/reference/any.md)
* [`any_respect_nulls`](/zh/sql-reference/aggregate-functions/reference/any.md)
* [`anyLast`](/zh/sql-reference/aggregate-functions/reference/anyLast.md)
* [`anyLast_respect_nulls`](/zh/sql-reference/aggregate-functions/reference/anyLast.md)
* [`min`](/zh/sql-reference/aggregate-functions/reference/min.md)
* [`max`](/zh/sql-reference/aggregate-functions/reference/max.md)
* [`sum`](/zh/sql-reference/aggregate-functions/reference/sum.md)
* [`sumWithOverflow`](/zh/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
* [`groupBitAnd`](/zh/sql-reference/aggregate-functions/reference/groupBitAnd.md)
* [`groupBitOr`](/zh/sql-reference/aggregate-functions/reference/groupBitOr.md)
* [`groupBitXor`](/zh/sql-reference/aggregate-functions/reference/groupBitXor.md)
* [`groupArrayArray`](/zh/sql-reference/aggregate-functions/reference/groupArrayArray.md)
* [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
* [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
* [`sumMap` (`sumMappedArrays`)](/zh/sql-reference/aggregate-functions/reference/sumMappedArrays.md)
* [`minMap` (`minMappedArrays`)](/zh/sql-reference/aggregate-functions/reference/minMappedArrays.md)
* [`maxMap` (`maxMappedArrays`)](/zh/sql-reference/aggregate-functions/reference/maxMappedArrays.md)

:::note
`SimpleAggregateFunction(func, Type)` 的值具有相同的 `Type`，
因此与 `AggregateFunction` 类型不同，无需应用
`-Merge`/`-State` 组合器。

对于相同的聚合函数，`SimpleAggregateFunction` 类型的性能比 `AggregateFunction` 更好。
:::

<div id="example">
  ## 示例
</div>

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

<div id="related-content">
  ## 相关内容
</div>

* 博客：[在 ClickHouse 中使用聚合组合器](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - 博客：[在 ClickHouse 中使用聚合组合器](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* [AggregateFunction](/zh/sql-reference/data-types/aggregatefunction) 类型。