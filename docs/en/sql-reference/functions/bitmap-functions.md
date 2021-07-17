---
toc_priority: 49
toc_title: Bitmap
---

# Bitmap Functions {#bitmap-functions}

Bitmap functions work for two bitmaps Object value calculation, it is to return new bitmap or cardinality while using formula calculation, such as and, or, xor, and not, etc.

There are 2 kinds of construction methods for Bitmap Object. One is to be constructed by aggregation function groupBitmap with -State, the other is to be constructed by Array Object. It is also to convert Bitmap Object to Array Object.

RoaringBitmap is wrapped into a data structure while actual storage of Bitmap objects. When the cardinality is less than or equal to 32, it uses Set objet. When the cardinality is greater than 32, it uses RoaringBitmap object. That is why storage of low cardinality set is faster.

For more information on RoaringBitmap, see: [CRoaring](https://github.com/RoaringBitmap/CRoaring).

## bitmapBuild {#bitmap_functions-bitmapbuild}

Build a bitmap from unsigned integer array.

``` sql
bitmapBuild(array)
```

**Arguments**

-   `array` – Unsigned integer array.

**Example**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res);
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)        │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray {#bitmaptoarray}

Convert bitmap to integer array.

``` sql
bitmapToArray(bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapSubsetInRange {#bitmap-functions-bitmapsubsetinrange}

Return subset in specified range (not include the range_end).

``` sql
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**Arguments**

-   `bitmap` – [Bitmap object](#bitmap_functions-bitmapbuild).
-   `range_start` – Range start point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `range_end` – Range end point (excluded). Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Example**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res;
```

``` text
┌─res───────────────┐
│ [30,31,32,33,100] │
└───────────────────┘
```

## bitmapSubsetLimit {#bitmapsubsetlimit}

Creates a subset of bitmap with n elements taken between `range_start` and `cardinality_limit`.

**Syntax**

``` sql
bitmapSubsetLimit(bitmap, range_start, cardinality_limit)
```

**Arguments**

-   `bitmap` – [Bitmap object](#bitmap_functions-bitmapbuild).
-   `range_start` – The subset starting point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `cardinality_limit` – The subset cardinality upper limit. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Returned value**

The subset.

Type: `Bitmap object`.

**Example**

Query:

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res;
```

Result:

``` text
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## bitmapContains {#bitmap_functions-bitmapcontains}

Checks whether the bitmap contains an element.

``` sql
bitmapContains(haystack, needle)
```

**Arguments**

-   `haystack` – [Bitmap object](#bitmap_functions-bitmapbuild), where the function searches.
-   `needle` – Value that the function searches. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**Returned values**

-   0 — If `haystack` does not contain `needle`.
-   1 — If `haystack` contains `needle`.

Type: `UInt8`.

**Example**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res;
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny {#bitmaphasany}

Checks whether two bitmaps have intersection by some elements.

``` sql
bitmapHasAny(bitmap1, bitmap2)
```

If you are sure that `bitmap2` contains strictly one element, consider using the [bitmapContains](#bitmap_functions-bitmapcontains) function. It works more efficiently.

**Arguments**

-   `bitmap*` – Bitmap object.

**Return values**

-   `1`, if `bitmap1` and `bitmap2` have one similar element at least.
-   `0`, otherwise.

**Example**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll {#bitmaphasall}

Analogous to `hasAll(array, array)` returns 1 if the first bitmap contains all the elements of the second one, 0 otherwise.
If the second argument is an empty bitmap then returns 1.

``` sql
bitmapHasAll(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│  0  │
└─────┘
```

## bitmapCardinality {#bitmapcardinality}

Retrun bitmap cardinality of type UInt64.

``` sql
bitmapCardinality(bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapMin {#bitmapmin}

Retrun the smallest value of type UInt64 in the set, UINT32_MAX if the set is empty.

    bitmapMin(bitmap)

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

``` text
 ┌─res─┐
 │   1 │
 └─────┘
```

## bitmapMax {#bitmapmax}

Retrun the greatest value of type UInt64 in the set, 0 if the set is empty.

    bitmapMax(bitmap)

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

``` text
 ┌─res─┐
 │   5 │
 └─────┘
```

## bitmapTransform {#bitmaptransform}

Transform an array of values in a bitmap to another array of values, the result is a new bitmap.

    bitmapTransform(bitmap, from_array, to_array)

**Arguments**

-   `bitmap` – Bitmap object.
-   `from_array` – UInt32 array. For idx in range \[0, from_array.size()), if bitmap contains from_array\[idx\], then replace it with to_array\[idx\]. Note that the result depends on array ordering if there are common elements between from_array and to_array.
-   `to_array` – UInt32 array, its size shall be the same to from_array.

**Example**

``` sql
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res;
```

``` text
 ┌─res───────────────────┐
 │ [1,3,4,6,7,8,9,10,20] │
 └───────────────────────┘
```

## bitmapAnd {#bitmapand}

Two bitmap and calculation, the result is a new bitmap.

``` sql
bitmapAnd(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

``` text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr {#bitmapor}

Two bitmap or calculation, the result is a new bitmap.

``` sql
bitmapOr(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor {#bitmapxor}

Two bitmap xor calculation, the result is a new bitmap.

``` sql
bitmapXor(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot {#bitmapandnot}

Two bitmap andnot calculation, the result is a new bitmap.

``` sql
bitmapAndnot(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapAndCardinality {#bitmapandcardinality}

Two bitmap and calculation, return cardinality of type UInt64.

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality {#bitmaporcardinality}

Two bitmap or calculation, return cardinality of type UInt64.

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality {#bitmapxorcardinality}

Two bitmap xor calculation, return cardinality of type UInt64.

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality {#bitmapandnotcardinality}

Two bitmap andnot calculation, return cardinality of type UInt64.

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**Arguments**

-   `bitmap` – Bitmap object.

**Example**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   2 │
└─────┘
```

