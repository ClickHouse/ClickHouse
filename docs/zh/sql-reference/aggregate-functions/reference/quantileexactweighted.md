---
sidebar_position: 203
---

# quantileExactWeighted {#quantileexactweighted}

考虑到每个元素的权重，然后准确计算数值序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

为了准确计算，所有输入的数据被合并为一个数组，并且部分的排序。每个输入值需要根据 `weight` 计算求和。该算法使用哈希表。正因为如此，在数据重复较多的时候使用的内存是少于[quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact)的。 您可以使用此函数代替 `quantileExact` 并指定`weight`为 1 。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用 [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 函数。

**语法**

``` sql
quantileExactWeighted(level)(expr, weight)
```

别名: `medianExactWeighted`。

**参数**
-   `level` — 分位数层次。可选参数。从0到1的一个float类型的常量。我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。当 `level=0.5` 时，该函数计算 [中位数](https://en.wikipedia.org/wiki/Median)。
-   `expr`  — 求值表达式，类型为数值类型[data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) 或 [DateTime](../../../sql-reference/data-types/datetime.md)。
-   `weight` — 权重序列。 权重是一个数据出现的数值。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

输入表:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

查询:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

结果:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**参见**

-   [中位数](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [分位数](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
