# Bitmap functions

Bitmap functions work for two bitmaps Object value calculation, it is to return new bitmap or cardinality while using formula calculation, such as and, or, xor, and not, etc.

There are 2 kinds of construction methods for Bitmap Object. One is to be constructed by aggregation function groupBitmap with -State, the other is to be constructed by Array Object. It is also to convert Bitmap Object to Array Object.

RoaringBitmap is wrapped into a data structure while actual storage of Bitmap objects. When the cardinality is less than or equal to 32, it uses Set objet. When the cardinality is greater than 32, it uses RoaringBitmap object. That is why storage of low cardinality set is faster. 

For more information on RoaringBitmap, see: [CRoaring](https://github.com/RoaringBitmap/CRoaring).


## bitmapBuild

Build a bitmap from unsigned integer array.

```
bitmapBuild(array)
```

**Parameters**

- `array` – unsigned integer array.

**Example**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res
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

## bitmapHasAny

Analogous to `hasAny(array, array)` returns 1 if bitmaps have any common elements, 0 otherwise.
For empty bitmaps returns 0.

```
bitmapHasAny(bitmap,bitmap)
```

**Parameters**

- `bitmap` – bitmap object.

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

## bitmapContains

Returns 1 if bitmap contains the given element, 0 otherwise.
For empty bitmaps returns 0.

```
bitmapContains(bitmap,element)
```

**Parameters**

- `bitmap` – bitmap object.
- `element` – UInt32 integer.

**Example**

``` sql
select bitmapContains(bitmapBuild([1,5,7,9]),CAST(9, 'UInt32')) AS res;
```

```
┌─res─┐
│  1  │
└─────┘
```


[Original article](https://clickhouse.yandex/docs/en/query_language/functions/bitmap_functions/) <!--hide-->
