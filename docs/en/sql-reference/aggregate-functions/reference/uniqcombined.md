---
toc_priority: 192
---

# uniqCombined {#agg_function-uniqcombined}

Calculates the approximate number of different argument values.

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

The `uniqCombined` function is a good choice for calculating the number of different values.

**Parameters**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

`HLL_precision` is the base-2 logarithm of the number of cells in [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Optional, you can use the function as `uniqCombined(x[, ...])`. The default value for `HLL_precision` is 17, which is effectively 96 KiB of space (2^17 cells, 6 bits each).

**Returned value**

-   A number [UInt64](../../../sql-reference/data-types/int-uint.md)-type number.

**Implementation details**

Function:

-   Calculates a hash (64-bit hash for `String` and 32-bit otherwise) for all parameters in the aggregate, then uses it in calculations.

-   Uses a combination of three algorithms: array, hash table, and HyperLogLog with an error correction table.

        For a small number of distinct elements, an array is used. When the set size is larger, a hash table is used. For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.

-   Provides the result deterministically (it doesn’t depend on the query processing order).

!!! note "Note"
    Since it uses 32-bit hash for non-`String` type, the result will have very high error for cardinalities significantly larger than `UINT_MAX` (error will raise quickly after a few tens of billions of distinct values), hence in this case you should use [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)

Compared to the [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) function, the `uniqCombined`:

-   Consumes several times less memory.
-   Calculates with several times higher accuracy.
-   Usually has slightly lower performance. In some scenarios, `uniqCombined` can perform better than `uniq`, for example, with distributed queries that transmit a large number of aggregation states over the network.

**See Also**

-   [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
-   [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
-   [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
