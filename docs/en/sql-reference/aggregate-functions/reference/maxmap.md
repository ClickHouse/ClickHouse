---
slug: /en/sql-reference/aggregate-functions/reference/maxmap
sidebar_position: 165
---

# maxMap

Calculates the maximum from `value` array according to the keys specified in the `key` array.

**Syntax**

```sql
maxMap(key, value)
```
or
```sql
maxMap(Tuple(key, value))
```

Alias: `maxMappedArrays`

:::note
- Passing a tuple of keys and value arrays is identical to passing two arrays of keys and values.
- The number of elements in `key` and `value` must be the same for each row that is totaled.
:::

**Parameters**

- `key` — Array of keys. [Array](../../data-types/array.md).
- `value` — Array of values. [Array](../../data-types/array.md).

**Returned value**

- Returns a tuple of two arrays: keys in sorted order, and values calculated for the corresponding keys. [Tuple](../../data-types/tuple.md)([Array](../../data-types/array.md), [Array](../../data-types/array.md)).

**Example**

Query:

``` sql
SELECT maxMap(a, b)
FROM values('a Array(Char), b Array(Int64)', (['x', 'y'], [2, 2]), (['y', 'z'], [3, 1]))
```

Result:

``` text
┌─maxMap(a, b)───────────┐
│ [['x','y','z'],[2,3,1]]│
└────────────────────────┘
```
