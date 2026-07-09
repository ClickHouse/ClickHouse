---
description: 'ClickHouse 中 AggregateFunction 数据类型的文档，用于存储聚合函数的中间状态'
keywords: ['AggregateFunction', '类型']
sidebar_label: 'AggregateFunction'
sidebar_position: 46
slug: /sql-reference/data-types/aggregatefunction
title: 'AggregateFunction 类型'
doc_type: 'reference'
---

<div id="description">
  ## 说明
</div>

ClickHouse 中所有[聚合函数](/zh/sql-reference/aggregate-functions)都有特定实现的中间状态，这种状态可以序列化为 `AggregateFunction` 数据类型并存储在表中。通常这是通过 [materialized view](../../sql-reference/statements/create/view.md) 实现的。

有两个聚合函数[组合器](/zh/sql-reference/aggregate-functions/combinators)通常会与 `AggregateFunction` 类型配合使用：

* [`-State`](/zh/sql-reference/aggregate-functions/combinators#-state) 聚合函数组合器，附加到聚合函数名称后，会生成 `AggregateFunction` 中间状态。
* [`-Merge`](/zh/sql-reference/aggregate-functions/combinators#-merge) 聚合
  函数组合器，用于从中间状态中得到聚合的最终结果。

<div id="syntax">
  ## 语法
</div>

```sql
AggregateFunction(aggregate_function_name, types_of_arguments...)
```

**参数**

* `aggregate_function_name` - 聚合函数的名称。如果该函数
  是参数化函数，还应指定其参数。
* `types_of_arguments` - 聚合函数参数的类型。

例如：

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

<div id="usage">
  ## 用量
</div>

<div id="data-insertion">
  ### 数据插入
</div>

要向包含 `AggregateFunction` 类型列的表中插入数据，您可以
使用 `INSERT SELECT`，并结合聚合函数以及
[`-State`](/zh/sql-reference/aggregate-functions/combinators#-state) 聚合
函数组合器。

例如，要向类型为 `AggregateFunction(uniq, UInt64)` 和
`AggregateFunction(quantiles(0.5, 0.9), UInt64)` 的列中插入数据，应使用以下
带组合器的聚合函数。

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

与函数 `uniq` 和 `quantiles` 不同，`uniqState` 和 `quantilesState`
(附加了 `-State` 组合器) 返回的是状态，而不是最终值。
换句话说，它们返回的是 `AggregateFunction` 类型的值。

在 `SELECT` 查询结果中，`AggregateFunction` 类型的值在所有 ClickHouse 输出
format 中都采用实现相关的二进制表示。

有一个特殊的会话级设置 `aggregate_function_input_format`，可用于根据输入值构建状态。
它支持以下格式：

* `state` - 包含序列化状态的二进制字符串 (默认值) 。
  例如，如果你使用 `SELECT`
  查询将数据转储为 `TabSeparated` 格式，那么这个转储可以通过 `INSERT` 查询再次加载。
* `value` - 该格式需要聚合函数参数的单个值；如果有多个参数，则需要它们组成的 Tuple；随后会将其反序列化为相应的状态
* `array` - 该格式需要一个值的 Array，如上面的 value 选项所述；数组中的所有元素都会被聚合以形成状态

<div id="data-selection">
  ### 数据选择
</div>

从 `AggregatingMergeTree` 表中选择数据时，请使用 `GROUP BY` 子句，
并使用与插入数据时相同的聚合函数，但要加上
[`-Merge`](/zh/sql-reference/aggregate-functions/combinators#-merge) 组合器。

为聚合函数附加 `-Merge` 组合器后，它会接收一组
状态，将其合并，并返回完整数据聚合的结果。

例如，以下两个查询会返回相同的结果：

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

<div id="usage-example">
  ## 用法示例
</div>

请参阅 [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 引擎的说明。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[在 ClickHouse 中使用适用于 Array、Map 和状态的聚合组合器](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* [MergeState](/zh/sql-reference/aggregate-functions/combinators#-mergestate)
  组合器。
* [State](/zh/sql-reference/aggregate-functions/combinators#-state) 组合器。