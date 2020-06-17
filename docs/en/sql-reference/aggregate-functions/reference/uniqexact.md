---
toc_priority: 191
---

# uniqExact {#agg_function-uniqexact}

Calculates the exact number of different argument values.

``` sql
uniqExact(x[, ...])
```

Use the `uniqExact` function if you absolutely need an exact result. Otherwise use the [uniq](uniq.md#agg_function-uniq) function.

The `uniqExact` function uses more memory than `uniq`, because the size of the state has unbounded growth as the number of different values increases.

**Parameters**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

**See Also**

-   [uniq](uniq.md#agg_function-uniq)
-   [uniqCombined](uniq.md#agg_function-uniqcombined)
-   [uniqHLL12](uniq.md#agg_function-uniqhll12)
