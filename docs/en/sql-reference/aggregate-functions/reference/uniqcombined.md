---
slug: /en/sql-reference/aggregate-functions/reference/uniqcombined
sidebar_position: 205
---

# uniqCombined

Calculates the approximate number of different argument values.

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

The `uniqCombined` function is a good choice for calculating the number of different values.

**Arguments**

- `HLL_precision`: The base-2 logarithm of the number of cells in [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Optional, you can use the function as `uniqCombined(x[, ...])`. The default value for `HLL_precision` is 17, which is effectively 96 KiB of space (2^17 cells, 6 bits each).
- `X`: A variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.


**Returned value**

- A number [UInt64](../../../sql-reference/data-types/int-uint.md)-type number.

**Implementation details**

The `uniqCombined` function:

- Calculates a hash (64-bit hash for `String` and 32-bit otherwise) for all parameters in the aggregate, then uses it in calculations.
- Uses a combination of three algorithms: array, hash table, and HyperLogLog with an error correction table.
    - For a small number of distinct elements, an array is used. 
    - When the set size is larger, a hash table is used. 
    - For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.
- Provides the result deterministically (it does not depend on the query processing order).

:::note    
Since it uses a 32-bit hash for non-`String` types, the result will have very high error for cardinalities significantly larger than `UINT_MAX` (error will raise quickly after a few tens of billions of distinct values), hence in this case you should use [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64).
:::

Compared to the [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) function, the `uniqCombined` function:

- Consumes several times less memory.
- Calculates with several times higher accuracy.
- Usually has slightly lower performance. In some scenarios, `uniqCombined` can perform better than `uniq`, for example, with distributed queries that transmit a large number of aggregation states over the network.

**Example**

Query:

```sql
SELECT uniqCombined(number) FROM numbers(1e6);
```

Result:

```response
┌─uniqCombined(number)─┐
│              1001148 │ -- 1.00 million
└──────────────────────┘
```

See the example section of [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64) for an example of the difference between `uniqCombined` and `uniqCombined64` for much larger inputs.

**See Also**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
- [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
- [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
