---
slug: /en/sql-reference/aggregate-functions/reference/uniqcombined64
sidebar_position: 206
---

# uniqCombined64

Calculates the approximate number of different argument values. It is the same as [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined), but uses a 64-bit hash for all data types rather than just for the String data type.

``` sql
uniqCombined64(HLL_precision)(x[, ...])
```

**Parameters**

- `HLL_precision`: The base-2 logarithm of the number of cells in [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Optionally, you can use the function as `uniqCombined64(x[, ...])`. The default value for `HLL_precision` is 17, which is effectively 96 KiB of space (2^17 cells, 6 bits each).
- `X`: A variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

**Returned value**

- A number [UInt64](../../../sql-reference/data-types/int-uint.md)-type number.

**Implementation details**

The `uniqCombined64` function:
- Calculates a hash (64-bit hash for all data types) for all parameters in the aggregate, then uses it in calculations.
- Uses a combination of three algorithms: array, hash table, and HyperLogLog with an error correction table.
    - For a small number of distinct elements, an array is used. 
    - When the set size is larger, a hash table is used. 
    - For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.
- Provides the result deterministically (it does not depend on the query processing order).

:::note
Since it uses 64-bit hash for all types, the result does not suffer from very high error for cardinalities significantly larger than `UINT_MAX` like [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md) does, which uses a 32-bit hash for non-`String` types.
:::

Compared to the [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) function, the `uniqCombined64` function:

- Consumes several times less memory.
- Calculates with several times higher accuracy.

**Example**

In the example below `uniqCombined64` is run on `1e10` different numbers returning a very close approximation of the number of different argument values. 

Query:

```sql
SELECT uniqCombined64(number) FROM numbers(1e10);
```

Result:

```response
┌─uniqCombined64(number)─┐
│             9998568925 │ -- 10.00 billion
└────────────────────────┘
```

By comparison the `uniqCombined` function returns a rather poor approximation for an input this size.

Query:

```sql
SELECT uniqCombined(number) FROM numbers(1e10);
```

Result:

```response
┌─uniqCombined(number)─┐
│           5545308725 │ -- 5.55 billion
└──────────────────────┘
```

**See Also**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md)
- [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
- [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
