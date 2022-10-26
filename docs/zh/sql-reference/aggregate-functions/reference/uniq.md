---
sidebar_position: 190
---

# uniq {#agg_function-uniq}

计算参数的不同值的近似数量。

**语法**

``` sql
uniq(x[, ...])
```

**参数**

该函数采用可变数量的参数。 参数可以是 `Tuple`, `Array`, `Date`, `DateTime`, `String`, 或数字类型。

**返回值**

-  [UInt64](../../../sql-reference/data-types/int-uint.md) 类型数值。

**实现细节**

功能:

-   计算聚合中所有参数的哈希值，然后在计算中使用它。

-   使用自适应采样算法。 对于计算状态，该函数使用最多65536个元素哈希值的样本。

    这个算法是非常精确的，并且对于CPU来说非常高效。如果查询包含一些这样的函数，那和其他聚合函数相比 `uniq` 将是几乎一样快。

-   确定性地提供结果（它不依赖于查询处理顺序）。

我们建议在几乎所有情况下使用此功能。

**参见**

-   [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
-   [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
-   [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
