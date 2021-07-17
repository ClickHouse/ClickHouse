---
toc_priority: 206
---

# quantileDeterministic {#quantiledeterministic}

计算数字序列的近似[分位数](https://en.wikipedia.org/wiki/Quantile)。

此功能适用 [水塘抽样](https://en.wikipedia.org/wiki/Reservoir_sampling)，使用储存器最大到8192和随机数发生器进行采样。 结果是非确定性的。 要获得精确的分位数，请使用 [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact) 功能。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)功能。

**语法**

``` sql
quantileDeterministic(level)(expr, determinator)
```

别名: `medianDeterministic`。

**参数**

-   `level` — 分位数层次。可选参数。从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`。默认值：0.5。 当 `level=0.5`时，该函数计算 [中位数](https://en.wikipedia.org/wiki/Median)。
-   `expr` — 求值表达式，类型为数值类型[data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) 或 [DateTime](../../../sql-reference/data-types/datetime.md)。
-   `determinator` — 一个数字，其hash被用来代替在水塘抽样中随机生成的数字，这样可以保证取样的确定性。你可以使用用户ID或者事件ID等任何正数，但是如果相同的 `determinator` 出现多次，那结果很可能不正确。
**返回值**

-   指定层次的近似分位数。

类型:

-   [Float64](../../../sql-reference/data-types/float.md) 用于数字数据类型输入。
-   [Date](../../../sql-reference/data-types/date.md) 如果输入值是 `Date` 类型。
-   [DateTime](../../../sql-reference/data-types/datetime.md) 如果输入值是 `DateTime` 类型。

**示例**

输入表:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

查询:

``` sql
SELECT quantileDeterministic(val, 1) FROM t
```

结果:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**参见**

-   [中位数](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [分位数](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
