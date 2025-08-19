---
slug: /zh/sql-reference/data-types/aggregatefunction
---
# AggregateFunction(name, types_of_arguments...) {#data-type-aggregatefunction}

聚合函数的中间状态，可以通过聚合函数名称加`-State`后缀的形式得到它。与此同时，当您需要访问该类型的最终状态数据时，您需要以相同的聚合函数名加`-Merge`后缀的形式来得到最终状态数据。

`AggregateFunction` — 参数化的数据类型。

**参数**

-   聚合函数名

        如果函数具备多个参数列表，请在此处指定其他参数列表中的值。

-   聚合函数参数的类型

**示例**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

上述中的[uniq](../../sql-reference/data-types/aggregatefunction.md#agg_function-uniq)， anyIf ([任何](../../sql-reference/data-types/aggregatefunction.md#agg_function-any)+[如果](../../sql-reference/data-types/aggregatefunction.md#agg-functions-combinator-if)) 以及 [分位数](../../sql-reference/data-types/aggregatefunction.md) 都为ClickHouse中支持的聚合函数。

## 使用指南 {#shi-yong-zhi-nan}

### 数据写入 {#shu-ju-xie-ru}

当需要写入数据时，您需要将数据包含在`INSERT SELECT`语句中，同时对于`AggregateFunction`类型的数据，您需要使用对应的以`-State`为后缀的函数进行处理。

**函数使用示例**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

不同于`uniq`和`quantiles`函数返回聚合结果的最终值，以`-State`后缀的函数总是返回`AggregateFunction`类型的数据的中间状态。

对于`SELECT`而言，`AggregateFunction`类型总是以特定的二进制形式展现在所有的输出格式中。例如，您可以使用`SELECT`语句将函数的状态数据转储为`TabSeparated`格式的同时使用`INSERT`语句将数据转储回去。

### 数据查询 {#shu-ju-cha-xun}

当从`AggregatingMergeTree`表中查询数据时，对于`AggregateFunction`类型的字段，您需要使用以`-Merge`为后缀的相同聚合函数来聚合数据。对于非`AggregateFunction`类型的字段，请将它们包含在`GROUP BY`子句中。

以`-Merge`为后缀的聚合函数，可以将多个`AggregateFunction`类型的中间状态组合计算为最终的聚合结果。

例如，如下的两个查询返回的结果总是一致：

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## 使用示例 {#shi-yong-shi-li}

请参阅 [AggregatingMergeTree](../../sql-reference/data-types/aggregatefunction.md) 的说明
