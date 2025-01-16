---
slug: /en/sql-reference/aggregate-functions/reference/uniqhllpp
sidebar_position: 208
---

# uniqHLLPP

Calculates the approximate number of different argument values, using the [HyperLogLog++](https://en.wikipedia.org/wiki/HyperLogLog#HLL++) algorithm. HyperLogLog++ is an advanced version of the HyperLogLog algorithm, which improves accuracy and reduces bias through the use of sparse representation and bias correction techniques. The algorithm is particularly efficient in terms of memory usage, making it suitable for large-scale data processing. The accuracy of the estimation can be adjusted by configuring the precision parameter, which controls the size of the data structure used by the algorithm. HyperLogLog++ is widely used in distributed systems for tasks such as counting unique visitors, tracking distinct events, and other analytics purposes.

``` sql
uniqHLLPP([relative_sd])(input)
```

**Arguments**

- relative_sd: The relative standard deviation, which controls the accuracy of the approximation. It is a floating-point number in range [0, 1)
- input: The input expression to be processed by the function. It must be of `UInt64` type.

**Returned value**

- A [UInt64](../../../sql-reference/data-types/int-uint.md)-type number.

**Implementation details**

Function:
- Uses the HyperLogLog++ algorithm to approximate the number of different argument values, assuming the input `UInt64` values are randomly distributed. If not, you should wrap the input column with `cityHash64`.
- Provides a determinate result (it does not depend on the query processing order).

**See Also**
- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
- [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)

