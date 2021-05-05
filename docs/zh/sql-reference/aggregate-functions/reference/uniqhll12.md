---
toc_priority: 194
---

# uniqHLL12 {#agg_function-uniqhll12}

计算不同参数值的近似数量，使用 [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) 算法。

**语法**

``` sql
uniqHLL12(x[, ...])
```

**参数**

该函数采用可变数量的参数。 参数可以是 `Tuple`, `Array`, `Date`, `DateTime`, `String`，或数字类型。

**返回值**

**返回值**

-   一个[UInt64](../../../sql-reference/data-types/int-uint.md)类型的数字。

**实现细节**

功能:

-   计算聚合中所有参数的哈希值，然后在计算中使用它。

-   使用 HyperLogLog 算法来近似不同参数值的数量。

        使用2^12个5比特单元。 状态的大小略大于2.5KB。 对于小数据集（<10K元素），结果不是很准确（误差高达10%）。 但是, 对于高基数数据集（10K-100M），结果相当准确，最大误差约为1.6%。Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   提供确定结果（它不依赖于查询处理顺序）。

我们不建议使用此函数。 在大多数情况下, 使用 [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) 或 [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined) 函数。

**参见**

-   [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
-   [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
-   [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
