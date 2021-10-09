---
toc_priority: 192
---

# uniqCombined {#agg_function-uniqcombined}

计算不同参数值的近似数量。

**语法**
``` sql
uniqCombined(HLL_precision)(x[, ...])
```
该 `uniqCombined` 函数是计算不同值数量的不错选择。

**参数**

该函数采用可变数量的参数。 参数可以是 `Tuple`, `Array`, `Date`, `DateTime`, `String`，或数字类型。

`HLL_precision` 是以2为底的单元格数的对数 [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog)。可选，您可以将该函数用作 `uniqCombined(x[, ...])`。 `HLL_precision` 的默认值是17，这是有效的96KiB的空间（2^17个单元，每个6比特）。

**返回值**

-   一个[UInt64](../../../sql-reference/data-types/int-uint.md)类型的数字。

**实现细节**

功能:

-   为聚合中的所有参数计算哈希（`String`类型用64位哈希，其他32位），然后在计算中使用它。

-   使用三种算法的组合：数组、哈希表和包含错误修正表的HyperLogLog。


    少量的不同的值，使用数组。 值再多一些，使用哈希表。对于大量的数据来说，使用HyperLogLog，HyperLogLog占用一个固定的内存空间。

-   确定性地提供结果（它不依赖于查询处理顺序）。

!!! note "注"
    由于它对非 `String` 类型使用32位哈希，对于基数显著大于`UINT_MAX` ，结果将有非常高的误差(误差将在几百亿不同值之后迅速提高), 因此这种情况，你应该使用 [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)

相比于 [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) 函数, 该 `uniqCombined`:

-   消耗内存要少几倍。
-   计算精度高出几倍。
-   通常具有略低的性能。 在某些情况下, `uniqCombined` 可以表现得比 `uniq` 好，例如，使用通过网络传输大量聚合状态的分布式查询。

**参见**

-   [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
-   [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
-   [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
