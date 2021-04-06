---
toc_priority: 191
---

# uniqExact {#agg_function-uniqexact}

计算不同参数值的准确数目。

**语法**

``` sql
uniqExact(x[, ...])
```
如果你绝对需要一个确切的结果，使用 `uniqExact` 函数。 否则使用 [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) 函数。

`uniqExact` 函数比 `uniq` 使用更多的内存，因为状态的大小随着不同值的数量的增加而无界增长。

**参数**

该函数采用可变数量的参数。 参数可以是 `Tuple`, `Array`, `Date`, `DateTime`, `String`，或数字类型。

**参见**

-   [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
-   [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniqcombined)
-   [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniqhll12)
