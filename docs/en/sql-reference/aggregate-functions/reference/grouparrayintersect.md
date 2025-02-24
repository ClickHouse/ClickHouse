---
slug: /en/sql-reference/aggregate-functions/reference/grouparrayintersect
sidebar_position: 141
---

# groupArrayIntersect

Return an intersection of given arrays (Return all items of arrays, that are in all given arrays).

**Syntax**

``` sql
groupArrayIntersect(x)
```

**Arguments**

- `x` — Argument (column name or expression).

**Returned values**

- Array that contains elements that are in all arrays.

Type: [Array](../../data-types/array.md).

**Examples**

Consider table `numbers`:

``` text
┌─a──────────────┐
│ [1,2,4]        │
│ [1,5,2,8,-1,0] │
│ [1,5,7,5,8,2]  │
└────────────────┘
```

Query with column name as argument:

``` sql
SELECT groupArrayIntersect(a) as intersection FROM numbers;
```

Result:

```text
┌─intersection──────┐
│ [1, 2]            │
└───────────────────┘
```
