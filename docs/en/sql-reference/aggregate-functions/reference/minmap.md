---
description: 'Calculates the minimum from `value` array according to the keys specified
  in the `key` array.'
sidebar_position: 169
slug: /sql-reference/aggregate-functions/reference/minmap
title: 'minMap'
---

# minMap

Calculates the minimum from `value` array according to the keys specified in the `key` array.

**Syntax**

```sql
`minMap(key, value)`
```
or
```sql
minMap(Tuple(key, value))
```

Alias: `minMappedArrays`

:::note
- Passing a tuple of keys and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and `value` must be the same for each row that is totaled.
:::

**Parameters**

- `key` — Array of keys. [Array](../../data-types/array.md).
- `value` — Array of values. [Array](../../data-types/array.md).

**Returned value**

- Returns a tuple of two arrays: keys in sorted order, and values calculated for the corresponding keys. [Tuple](../../data-types/tuple.md)([Array](../../data-types/array.md), [Array](../../data-types/array.md)).

**Example**

Query:

```sql
SELECT minMap(a, b)
FROM VALUES('a Array(Int32), b Array(Int64)', ([1, 2], [2, 2]), ([2, 3], [1, 1]))
```

Result:

```text
┌─minMap(a, b)──────┐
│ ([1,2,3],[2,1,1]) │
└───────────────────┘
```
