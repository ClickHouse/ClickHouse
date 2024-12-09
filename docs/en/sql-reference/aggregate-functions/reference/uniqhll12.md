---
slug: /en/sql-reference/aggregate-functions/reference/uniqhll12
sidebar_position: 208
---

# uniqHLL12

Calculates the approximate number of different argument values, using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm.

``` sql
uniqHLL12(x[, ...])
```

**Arguments**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

**Returned value**

- A [UInt64](../../../sql-reference/data-types/int-uint.md)-type number.

**Implementation details**

Function:

- Calculates a hash for all parameters in the aggregate, then uses it in calculations.

- Uses the HyperLogLog algorithm to approximate the number of different argument values.

        2^12 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

- Provides the determinate result (it does not depend on the query processing order).

We do not recommend using this function. In most cases, use the [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) or [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined) function.

**See Also**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
- [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
