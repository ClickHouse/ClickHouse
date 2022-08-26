---
toc_priority: 207
---

# quantileTDigest {#quantiletdigest}

使用[t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) 算法计算数字序列近似[分位数](https://en.wikipedia.org/wiki/Quantile)。

最大误差为1%。 内存消耗为 `log(n)`，这里 `n` 是值的个数。 结果取决于运行查询的顺序，并且是不确定的。

该函数的性能低于 [quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile) 或 [quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming.md#quantiletiming) 的性能。 从状态大小和精度的比值来看，这个函数比 `quantile` 更优秀。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用 [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 函数。

**语法**

``` sql
quantileTDigest(level)(expr)
```

别名: `medianTDigest`。

**参数**

-   `level` — 分位数层次。可选参数。从0到1的一个float类型的常量。我们推荐 `level` 值的范围为 `[0.01, 0.99]` 。默认值：0.5。当 `level=0.5` 时，该函数计算 [中位数](https://en.wikipedia.org/wiki/Median)。
-   `expr`  — 求值表达式，类型为数值类型[data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) 或 [DateTime](../../../sql-reference/data-types/datetime.md)。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../../sql-reference/data-types/float.md) 用于数字数据类型输入。
-   [Date](../../../sql-reference/data-types/date.md) 如果输入值是 `Date` 类型。
-   [DateTime](../../../sql-reference/data-types/datetime.md) 如果输入值是 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

结果:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**参见**

-   [中位数](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [分位数](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
