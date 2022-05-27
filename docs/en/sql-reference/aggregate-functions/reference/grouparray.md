---
sidebar_position: 110
---

# groupArray {#agg_function-grouparray}

Syntax: `groupArray(x)` or `groupArray(max_size)(x)`

Creates an array of argument values.
Values can be added to the array in any (indeterminate) order.

The second version (with the `max_size` parameter) limits the size of the resulting array to `max_size` elements. For example, `groupArray(1)(x)` is equivalent to `[any (x)]`.

In some cases, you can still rely on the order of execution. This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY`.
