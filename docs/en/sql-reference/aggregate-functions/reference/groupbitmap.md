---
slug: /en/sql-reference/aggregate-functions/reference/groupbitmap
sidebar_position: 148
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
