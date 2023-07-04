---
sidebar_position: 205
---

# quantileTimingWeighted {#quantiletimingweighted}

根据每个序列成员的权重，使用确定的精度计算数字序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

结果是确定性的（它不依赖于查询处理顺序）。该函数针对描述加载网页时间或后端响应时间等分布的序列进行了优化。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)功能。

**语法**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

别名: `medianTimingWeighted`。

**参数**

-   `level` — 分位数层次。可选参数。从0到1的一个float类型的常量。我们推荐 `level` 值的范围为 `[0.01, 0.99]` 。默认值：0.5。当 `level=0.5` 时，该函数计算 [中位数](https://en.wikipedia.org/wiki/Median)。
-   `expr` — 求值[表达式](../../../sql-reference/syntax.md#syntax-expressions) 返回 [Float\*](../../../sql-reference/data-types/float.md) 类型数值。

    - 如果输入负值，那结果是不可预期的。
    - 如果输入值大于30000（页面加载时间大于30s），那我们假设为30000。

-   `weight` — 权重序列。 权重是一个数据出现的数值。

**精度**

计算是准确的，如果:


-   值的总数不超过5670。
-   总数值超过5670，但页面加载时间小于1024ms。

否则，计算结果将四舍五入到16毫秒的最接近倍数。

!!! note "注"
    对于计算页面加载时间分位数， 此函数比[quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile)更有效和准确。

**返回值**

-   指定层次的分位数。

类型: `Float32`。

!!! note "注"
    如果没有值传递给函数（当使用 `quantileTimingIf`), [NaN](../../../sql-reference/data-types/float.md#data_type-float-nan-inf)被返回。 这样做的目的是将这些案例与导致零的案例区分开来。 参见 [ORDER BY clause](../../../sql-reference/statements/select/order-by.md#select-order-by) 对于 `NaN` 值排序注意事项。

**示例**

输入表:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

查询:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

结果:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

# quantilesTimingWeighted {#quantilestimingweighted}

类似于 `quantileTimingWeighted` , 但接受多个分位数层次参数，并返回一个由这些分位数值组成的数组。

**示例**

输入表:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

查询:

``` sql
SELECT quantilesTimingWeighted(0,5, 0.99)(response_time, weight) FROM t
```

结果:

``` text
┌─quantilesTimingWeighted(0.5, 0.99)(response_time, weight)─┐
│ [112,162]                                                 │
└───────────────────────────────────────────────────────────┘
```

**参见**

-   [中位数](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [分位数](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
