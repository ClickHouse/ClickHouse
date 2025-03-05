---
slug: /sql-reference/aggregate-functions/reference/groupbitor
sidebar_position: 152
title: "groupBitOr"
description: "Applies bit-wise `OR` to a series of numbers."
---

# groupBitOr

Applies bit-wise `OR` to a series of numbers.

``` sql
groupBitOr(expr)
```

**Arguments**

`expr` – An expression that results in `UInt*` or `Int*` type.

**Returned value**

Value of the `UInt*` or `Int*` type.

**Example**

Test data:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Query:

``` sql
SELECT groupBitOr(num) FROM t
```

Where `num` is the column with the test data.

Result:

``` text
binary     decimal
01111101 = 125
```

## Combinators

The following combinators can be applied to the `groupBitOr` function:

### groupBitOrIf
Applies bit-wise OR only to rows that match the given condition.

### groupBitOrArray
Applies bit-wise OR to elements in the array.

### groupBitOrMap
Applies bit-wise OR to values for each key in the map separately.

### groupBitOrSimpleState
Returns the bit-wise OR value with SimpleAggregateFunction type.

### groupBitOrState
Returns the intermediate state of bit-wise OR calculation.

### groupBitOrMerge
Combines intermediate bit-wise OR states to get the final result.

### groupBitOrMergeState
Combines intermediate bit-wise OR states but returns an intermediate state.

### groupBitOrForEach
Applies bit-wise OR to corresponding elements in multiple arrays.

### groupBitOrDistinct
Applies bit-wise OR to distinct values only.

### groupBitOrOrDefault
Returns 0 if there are no rows to apply bit-wise OR.

### groupBitOrOrNull
Returns NULL if there are no rows to apply bit-wise OR.
