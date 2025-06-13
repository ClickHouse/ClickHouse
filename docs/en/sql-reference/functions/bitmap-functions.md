---
slug: /en/sql-reference/functions/bitmap-functions
sidebar_position: 25
sidebar_label: Bitmap
---

# Bitmap Functions

Bitmaps can be constructed in two ways. The first way is constructed by aggregation function groupBitmap with `-State`, the other way is to constructed a bitmap from an Array object.

## bitmapBuild

Builds a bitmap from an unsigned integer array.

**Syntax**

``` sql
bitmapBuild(array)
```

**Arguments**

- `array` – Unsigned integer array.

**Example**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res);
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)        │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray

Converts bitmap to an integer array.

**Syntax**

``` sql
bitmapToArray(bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

Result:

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapSubsetInRange

Returns the subset of a bitmap with bits within a value interval.

**Syntax**

``` sql
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**Arguments**

- `bitmap` – [Bitmap object](#bitmapbuild).
- `range_start` – Start of the range (inclusive). [UInt32](../data-types/int-uint.md).
- `range_end` – End of the range (exclusive). [UInt32](../data-types/int-uint.md).

**Example**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res;
```

Result:

``` text
┌─res───────────────┐
│ [30,31,32,33,100] │
└───────────────────┘
```

## bitmapSubsetLimit

Returns a subset of a bitmap with smallest bit value `range_start` and at most `cardinality_limit` elements.

**Syntax**

``` sql
bitmapSubsetLimit(bitmap, range_start, cardinality_limit)
```

**Arguments**

- `bitmap` – [Bitmap object](#bitmapbuild).
- `range_start` – Start of the range (inclusive). [UInt32](../data-types/int-uint.md).
- `cardinality_limit` – Maximum cardinality of the subset. [UInt32](../data-types/int-uint.md).

**Example**

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res;
```

Result:

``` text
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## subBitmap

Returns a subset of the bitmap, starting from position `offset`. The maximum cardinality of the returned bitmap is `cardinality_limit`.

**Syntax**

``` sql
subBitmap(bitmap, offset, cardinality_limit)
```

**Arguments**

- `bitmap` – The bitmap. [Bitmap object](#bitmapbuild).
- `offset` – The position of the first element of the subset. [UInt32](../data-types/int-uint.md).
- `cardinality_limit` – The maximum number of elements in the subset. [UInt32](../data-types/int-uint.md).

**Example**

``` sql
SELECT bitmapToArray(subBitmap(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(10), toUInt32(10))) AS res;
```

Result:

``` text
┌─res─────────────────────────────┐
│ [10,11,12,13,14,15,16,17,18,19] │
└─────────────────────────────────┘
```

## bitmapContains

Checks whether the bitmap contains an element.

``` sql
bitmapContains(bitmap, needle)
```

**Arguments**

- `bitmap` – [Bitmap object](#bitmapbuild).
- `needle` – Searched bit value. [UInt32](../data-types/int-uint.md).

**Returned values**

- 0 — If `bitmap` does not contain `needle`. [UInt8](../data-types/int-uint.md).
- 1 — If `bitmap` contains `needle`. [UInt8](../data-types/int-uint.md).

**Example**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res;
```

Result:

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny

Checks whether two bitmaps intersect.

If `bitmap2` contains exactly one element, consider using [bitmapContains](#bitmapcontains) instead as it works more efficiently.

**Syntax**

``` sql
bitmapHasAny(bitmap1, bitmap2)
```

**Arguments**

- `bitmap1` – Bitmap object 1.
- `bitmap2` – Bitmap object 2.

**Return values**

- `1`, if `bitmap1` and `bitmap2` have at least one shared element.
- `0`, otherwise.

**Example**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

Result:

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll

Returns 1 if the first bitmap contains all elements of the second bitmap, otherwise 0.
If the second bitmap is empty, returns 1.

Also see `hasAll(array, array)`.

**Syntax**

``` sql
bitmapHasAll(bitmap1, bitmap2)
```

**Arguments**

- `bitmap1` – Bitmap object 1.
- `bitmap2` – Bitmap object 2.

**Example**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

Result:

``` text
┌─res─┐
│  0  │
└─────┘
```

## bitmapCardinality

Returns the cardinality of a bitmap.

**Syntax**

``` sql
bitmapCardinality(bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

Result:

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapMin

Computes the smallest bit set in a bitmap, or UINT32_MAX if the bitmap is empty.

**Syntax**

```sql 
bitmapMin(bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

Result:

``` text
 ┌─res─┐
 │   1 │
 └─────┘
```

## bitmapMax

Computes the greatest bit set in a bitmap, or 0 if the bitmap is empty.

**Syntax**

```sql 
bitmapMax(bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

Result:

``` text
 ┌─res─┐
 │   5 │
 └─────┘
```

## bitmapTransform

Replaces at most N bits in a bitmap. The old and new value of the i-th replaced bit is given by `from_array[i]` and `to_array[i]`.

The result depends on the array ordering if `from_array` and `to_array`.

**Syntax**

``` sql
bitmapTransform(bitmap, from_array, to_array)
```

**Arguments**

- `bitmap` – Bitmap object.
- `from_array` – UInt32 array. For idx in range \[0, from_array.size()), if bitmap contains from_array\[idx\], then replace it with to_array\[idx\].
- `to_array` – UInt32 array with the same size as `from_array`.

**Example**

``` sql
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res;
```

Result:

``` text
 ┌─res───────────────────┐
 │ [1,3,4,6,7,8,9,10,20] │
 └───────────────────────┘
```

## bitmapAnd

Computes the logical conjunction of two bitmaps.

**Syntax**

``` sql
bitmapAnd(bitmap,bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

Result:

``` text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr

Computes the logical disjunction of two bitmaps.

**Syntax**

``` sql
bitmapOr(bitmap,bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

Result:

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor

Xor-s two bitmaps.

**Syntax**

``` sql
bitmapXor(bitmap,bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

Result:

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot

Computes the logical conjunction of two bitmaps and negates the result.

**Syntax**

``` sql
bitmapAndnot(bitmap,bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

Result:

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapAndCardinality

Returns the cardinality of the logical conjunction of two bitmaps.

**Syntax**

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

Result:

``` text
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality

Returns the cardinality of the logical disjunction of two bitmaps.

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

Result:

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality

Returns the cardinality of the XOR of two bitmaps.

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

Result:

``` text
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality

Returns the cardinality of the AND-NOT operation of two bitmaps.

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**Arguments**

- `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

Result:

``` text
┌─res─┐
│   2 │
└─────┘
```
