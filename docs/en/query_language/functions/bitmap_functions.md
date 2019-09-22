# Bitmap functions

Bitmap functions work for two bitmaps Object value calculation, it is to return new bitmap or cardinality while using formula calculation, such as and, or, xor, and not, etc.

There are 2 kinds of construction methods for Bitmap Object. One is to be constructed by aggregation function groupBitmap with -State, the other is to be constructed by Array Object. It is also to convert Bitmap Object to Array Object.

RoaringBitmap is wrapped into a data structure while actual storage of Bitmap objects. When the cardinality is less than or equal to 32, it uses Set objet. When the cardinality is greater than 32, it uses RoaringBitmap object. That is why storage of low cardinality set is faster.

For more information on RoaringBitmap, see: [CRoaring](https://github.com/RoaringBitmap/CRoaring).


## bitmapBuild {#bitmap_functions-bitmapbuild}

Build a bitmap from unsigned integer array.

```
bitmapBuild(array)
```

**Parameters**

- `array` – unsigned integer array.

**Example**

```sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res)
```
```text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)    │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray

Convert bitmap to integer array.

```
bitmapToArray(bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

```
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapSubsetInRange {#bitmap_functions-bitmapsubsetinrange}

Return subset in specified range (not include the range_end).

```
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**Parameters**

- `bitmap` – [Bitmap object](#bitmap_functions-bitmapbuild).
- `range_start` – range start point. Type: [UInt32](../../data_types/int_uint.md).
- `range_end` – range end point(excluded). Type: [UInt32](../../data_types/int_uint.md).

**Example**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

```
┌─res───────────────┐
│ [30,31,32,33,100] │
└───────────────────┘
```

## bitmapSubsetLimit {#bitmap_functions-bitmapsubsetlimit}

Return subset of the smallest `limit` values in set which is no less than `range_start`.

```
bitmapSubsetLimit(bitmap, range_start, limit)
```

**Parameters**

- `bitmap` – [Bitmap object](#bitmap_functions-bitmapbuild).
- `range_start` – range start point. Type: [UInt32](../../data_types/int_uint.md).
- `limit` – subset cardinality upper limit. Type: [UInt32](../../data_types/int_uint.md).

**Example**

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

```
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## bitmapContains {#bitmap_functions-bitmapcontains}

Checks whether the bitmap contains an element.

```
bitmapContains(haystack, needle)
```

**Parameters**

- `haystack` – [Bitmap object](#bitmap_functions-bitmapbuild), where the function searches.
- `needle` – Value that the function searches. Type: [UInt32](../../data_types/int_uint.md).

**Returned values**

- 0 — If `haystack` doesn't contain `needle`.
- 1 — If `haystack` contains `needle`.

Type: `UInt8`.

**Example**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res
```
```text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny

Checks whether two bitmaps have intersection by some elements.

```
bitmapHasAny(bitmap1, bitmap2)
```

If you are sure that `bitmap2` contains strictly one element, consider using the [bitmapContains](#bitmap_functions-bitmapcontains) function. It works more efficiently.

**Parameters**

- `bitmap*` – bitmap object.

**Return values**

- `1`, if `bitmap1` and `bitmap2` have one similar element at least.
- `0`, otherwise.

**Example**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

```
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll

Analogous to `hasAll(array, array)` returns 1 if the first bitmap contains all the elements of the second one, 0 otherwise.
If the second argument is an empty bitmap then returns 1.

```
bitmapHasAll(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

```
┌─res─┐
│  0  │
└─────┘
```


## bitmapAnd

Two bitmap and calculation, the result is a new bitmap.

```
bitmapAnd(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```
┌─res─┐
│ [3] │
└─────┘
```


## bitmapOr

Two bitmap or calculation, the result is a new bitmap.

```
bitmapOr(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor

Two bitmap xor calculation, the result is a new bitmap.

```
bitmapXor(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot

Two bitmap andnot calculation, the result is a new bitmap.

```
bitmapAndnot(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

```
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapCardinality

Retrun bitmap cardinality of type UInt64.


```
bitmapCardinality(bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

```
┌─res─┐
│   5 │
└─────┘
```

## bitmapMin

Retrun smallest value of type UInt64 in the set, UINT32_MAX if the set is empty.


```
bitmapMin(bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

```
┌─res─┐
│   1 │
└─────┘
```

## bitmapMax

Retrun smallest value of type UInt64 in the set, 0 if the set is empty.


```
bitmapMax(bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

```
┌─res─┐
│   5 │
└─────┘
```

## bitmapAndCardinality

Two bitmap and calculation, return cardinality of type UInt64.


```
bitmapAndCardinality(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```
┌─res─┐
│   1 │
└─────┘
```


## bitmapOrCardinality

Two bitmap or calculation, return cardinality of type UInt64.

```
bitmapOrCardinality(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality

Two bitmap xor calculation, return cardinality of type UInt64.

```
bitmapXorCardinality(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```
┌─res─┐
│   4 │
└─────┘
```


## bitmapAndnotCardinality

Two bitmap andnot calculation, return cardinality of type UInt64.

```
bitmapAndnotCardinality(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

**Example**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

```
┌─res─┐
│   2 │
└─────┘
```


[Original article](https://clickhouse.yandex/docs/en/query_language/functions/bitmap_functions/) <!--hide-->
