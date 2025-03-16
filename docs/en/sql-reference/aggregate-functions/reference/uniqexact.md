---
slug: /en/sql-reference/aggregate-functions/reference/uniqexact
sidebar_position: 207
---

# uniqExact

Calculates the exact number of different argument values.

``` sql
uniqExact(x[, ...])
```

Use the `uniqExact` function if you absolutely need an exact result. Otherwise use the [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) function.

The `uniqExact` function uses more memory than `uniq`, because the size of the state has unbounded growth as the number of different values increases.

**Arguments**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

**See Also**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniqcombined)
- [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniqhll12)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
