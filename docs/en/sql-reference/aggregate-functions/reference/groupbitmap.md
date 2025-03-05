---
slug: /sql-reference/aggregate-functions/reference/groupbitmap
sidebar_position: 148
title: "groupBitmap"
description: "Bitmap or Aggregate calculations from a unsigned integer column, return cardinality of type UInt64, if add suffix -State, then return a bitmap object"
---

# groupBitmap

Bitmap or Aggregate calculations from a unsigned integer column, return cardinality of type UInt64, if add suffix -State, then return [bitmap object](../../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**Arguments**

`expr` – An expression that results in `UInt*` type.

**Return value**

Value of the `UInt64` type.

**Example**

Test data:

``` text
UserID
1
1
2
3
```

Query:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

Result:

``` text
num
3
```

## Combinators

The following combinators can be applied to the `groupBitmap` function:

### groupBitmapIf
Creates a bitmap only from rows that match the given condition.

### groupBitmapArray
Creates a bitmap from elements in the array.

### groupBitmapMap
Creates a bitmap for each key in the map separately.

### groupBitmapSimpleState
Returns the bitmap with SimpleAggregateFunction type.

### groupBitmapState
Returns the intermediate state of bitmap calculation.

### groupBitmapMerge
Combines intermediate bitmap states to get the final bitmap.

### groupBitmapMergeState
Combines intermediate bitmap states but returns an intermediate state.

### groupBitmapForEach
Creates bitmaps from corresponding elements in multiple arrays.

### groupBitmapDistinct
Creates a bitmap from distinct values only (same as regular groupBitmap).

### groupBitmapOrDefault
Returns an empty bitmap (cardinality 0) if there are no rows.

### groupBitmapOrNull
Returns NULL if there are no rows.
