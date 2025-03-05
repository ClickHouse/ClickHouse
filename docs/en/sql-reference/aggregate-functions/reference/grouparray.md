---
slug: /sql-reference/aggregate-functions/reference/grouparray
sidebar_position: 139
title: "groupArray"
description: "Creates an array of argument values. Values can be added to the array in any (indeterminate) order."
---

# groupArray

Syntax: `groupArray(x)` or `groupArray(max_size)(x)`

Creates an array of argument values.
Values can be added to the array in any (indeterminate) order.

The second version (with the `max_size` parameter) limits the size of the resulting array to `max_size` elements. For example, `groupArray(1)(x)` is equivalent to `[any (x)]`.

In some cases, you can still rely on the order of execution. This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY` if the subquery result is small enough.

**Example**

``` text
SELECT * FROM default.ck;

┌─id─┬─name─────┐
│  1 │ zhangsan │
│  1 │ ᴺᵁᴸᴸ     │
│  1 │ lisi     │
│  2 │ wangwu   │
└────┴──────────┘

```

Query:

``` sql
select id, groupArray(10)(name) from default.ck group by id;
```

Result:

``` text
┌─id─┬─groupArray(10)(name)─┐
│  1 │ ['zhangsan','lisi']  │
│  2 │ ['wangwu']           │
└────┴──────────────────────┘
```

The groupArray function will remove ᴺᵁᴸᴸ value based on the above results.

- Alias: `array_agg`.

## Combinators

The following combinators can be applied to the `groupArray` function:

### groupArrayIf
Creates an array of values only for rows that match the given condition.

### groupArrayArray
Creates an array of arrays by concatenating input arrays.

### groupArrayMap
Creates an array of values for each key in the map separately.

### groupArraySimpleState
Returns the array with SimpleAggregateFunction type.

### groupArrayState
Returns the intermediate state of array collection.

### groupArrayMerge
Combines intermediate array states to get the final array.

### groupArrayMergeState
Combines intermediate array states but returns an intermediate state.

### groupArrayForEach
Creates arrays by collecting corresponding elements from multiple input arrays.

### groupArrayDistinct
Creates an array of distinct values only.

### groupArrayOrDefault
Returns an empty array if there are no rows to collect.

### groupArrayOrNull
Returns NULL if there are no rows to collect.
