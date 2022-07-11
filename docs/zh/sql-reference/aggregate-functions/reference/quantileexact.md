---
sidebar_position: 202
---

# quantileExact {#quantileexact}


准确计算数字序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

为了准确计算，所有输入的数据被合并为一个数组，并且部分的排序。因此该函数需要 `O(n)` 的内存，n为输入数据的个数。但是对于少量数据来说，该函数还是非常有效的。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用 [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 函数。

**语法**

``` sql
quantileExact(level)(expr)
```

别名: `medianExact`。

**参数**

-   `level` — 分位数层次。可选参数。从0到1的一个float类型的常量。我们推荐 `level` 值的范围为 `[0.01, 0.99]`。默认值：0.5。当 `level=0.5` 时，该函数计算[中位数](https://en.wikipedia.org/wiki/Median)。
-   `expr` — 求值表达式，类型为数值类型[data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) 或 [DateTime](../../../sql-reference/data-types/datetime.md)。

**返回值**

-   指定层次的分位数。


类型:

-   [Float64](../../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

结果:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

# quantileExactLow {#quantileexactlow}

和 `quantileExact` 相似, 准确计算数字序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

为了准确计算，所有输入的数据被合并为一个数组，并且全排序。这排序[算法](https://en.cppreference.com/w/cpp/algorithm/sort)的复杂度是 `O(N·log(N))`, 其中 `N = std::distance(first, last)` 比较。

返回值取决于分位数级别和所选取的元素数量，即如果级别是 0.5, 函数返回偶数元素的低位中位数，奇数元素的中位数。中位数计算类似于 python 中使用的[median_low](https://docs.python.org/3/library/statistics.html#statistics.median_low)的实现。

对于所有其他级别， 返回 `level * size_of_array` 值所对应的索引的元素值。

例如:

``` sql
SELECT quantileExactLow(0.1)(number) FROM numbers(10)

┌─quantileExactLow(0.1)(number)─┐
│                             1 │
└───────────────────────────────┘
```

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用 [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 函数。

**语法**

``` sql
quantileExactLow(level)(expr)
```

别名: `medianExactLow`。

**参数**

-   `level` — 分位数层次。可选参数。从0到1的一个float类型的常量。我们推荐 `level` 值的范围为 `[0.01, 0.99]`。默认值：0.5。当 `level=0.5` 时，该函数计算 [中位数](https://en.wikipedia.org/wiki/Median)。
-   `expr` — — 求值表达式，类型为数值类型[data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) 或 [DateTime](../../../sql-reference/data-types/datetime.md)。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../../sql-reference/data-types/float.md) 用于数字数据类型输入。
-   [Date](../../../sql-reference/data-types/date.md) 如果输入值是 `Date` 类型。
-   [DateTime](../../../sql-reference/data-types/datetime.md) 如果输入值是 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileExactLow(number) FROM numbers(10)
```

结果:

``` text
┌─quantileExactLow(number)─┐
│                        4 │
└──────────────────────────┘
```

# quantileExactHigh {#quantileexacthigh}

和 `quantileExact` 相似, 准确计算数字序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

为了准确计算，所有输入的数据被合并为一个数组，并且全排序。这排序[算法](https://en.cppreference.com/w/cpp/algorithm/sort)的复杂度是 `O(N·log(N))`, 其中 `N = std::distance(first, last)` 比较。

返回值取决于分位数级别和所选取的元素数量，即如果级别是 0.5, 函数返回偶数元素的低位中位数，奇数元素的中位数。中位数计算类似于 python 中使用的[median_high](https://docs.python.org/3/library/statistics.html#statistics.median_high)的实现。

对于所有其他级别， 返回 `level * size_of_array` 值所对应的索引的元素值。

这个实现与当前的 `quantileExact` 实现完全相似。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用 [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 函数。

**语法**

``` sql
quantileExactHigh(level)(expr)
```

别名: `medianExactHigh`。

**参数**

-   `level` — 分位数层次。可选参数。从0到1的一个float类型的常量。我们推荐 `level` 值的范围为 `[0.01, 0.99]`。默认值：0.5。当 `level=0.5` 时，该函数计算 [中位数](https://en.wikipedia.org/wiki/Median)。
-   `expr` — — 求值表达式，类型为数值类型[data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) 或 [DateTime](../../../sql-reference/data-types/datetime.md)。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../../sql-reference/data-types/float.md) 用于数字数据类型输入。
-   [Date](../../../sql-reference/data-types/date.md) 如果输入值是 `Date` 类型。
-   [DateTime](../../../sql-reference/data-types/datetime.md) 如果输入值是 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileExactHigh(number) FROM numbers(10)
```

结果:

``` text
┌─quantileExactHigh(number)─┐
│                         5 │
└───────────────────────────┘
```
**参见**

-   [中位数](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [分位数](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
