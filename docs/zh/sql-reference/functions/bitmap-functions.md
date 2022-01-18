# 位图函数 {#wei-tu-han-shu}

位图函数用于对两个位图对象进行计算，对于任何一个位图函数，它都将返回一个位图对象，例如and，or，xor，not等等。

位图对象有两种构造方法。一个是由聚合函数groupBitmapState构造的，另一个是由Array Object构造的。同时还可以将位图对象转化为数组对象。

我们使用RoaringBitmap实际存储位图对象，当基数小于或等于32时，它使用Set保存。当基数大于32时，它使用RoaringBitmap保存。这也是为什么低基数集的存储更快的原因。

有关RoaringBitmap的更多信息，请参阅：[RoaringBitmap](https://github.com/RoaringBitmap/CRoaring)。

## bitmapBuild {#bitmapbuild}

从无符号整数数组构建位图对象。

    bitmapBuild(array)

**参数**

-   `array` – 无符号整数数组.

**示例**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res
```

## bitmapToArray {#bitmaptoarray}

将位图转换为整数数组。

    bitmapToArray(bitmap)

**参数**

-   `bitmap` – 位图对象.

**示例**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─────────┐
    │ [1,2,3,4,5] │
    └─────────────┘

## bitmapSubsetInRange {#bitmapsubsetinrange}

将位图指定范围（不包含range_end）转换为另一个位图。

    bitmapSubsetInRange(bitmap, range_start, range_end)

**参数**

-   `bitmap` – 位图对象.
-   `range_start` – 范围起始点（含）.
-   `range_end` – 范围结束点（不含）.

**示例**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

    ┌─res───────────────┐
    │ [30,31,32,33,100] │
    └───────────────────┘

## bitmapSubsetLimit {#bitmapsubsetlimit}

将位图指定范围（起始点和数目上限）转换为另一个位图。

    bitmapSubsetLimit(bitmap, range_start, limit)

**参数**

-   `bitmap` – 位图对象.
-   `range_start` – 范围起始点（含）.
-   `limit` – 子位图基数上限.

**示例**

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

    ┌─res───────────────────────┐
    │ [30,31,32,33,100,200,500] │
    └───────────────────────────┘

## bitmapContains {#bitmapcontains}

检查位图是否包含指定元素。

    bitmapContains(haystack, needle)

**参数**

-   `haystack` – 位图对象.
-   `needle` – 元素，类型UInt32.

**示例**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny {#bitmaphasany}

与`hasAny(array，array)`类似，如果位图有任何公共元素则返回1，否则返回0。
对于空位图，返回0。

    bitmapHasAny(bitmap,bitmap)

**参数**

-   `bitmap` – bitmap对象。

**示例**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

    ┌─res─┐
    │  1  │
    └─────┘

## bitmapHasAll {#bitmaphasall}

与`hasAll(array，array)`类似，如果第一个位图包含第二个位图的所有元素，则返回1，否则返回0。
如果第二个参数是空位图，则返回1。

    bitmapHasAll(bitmap,bitmap)

**参数**

-   `bitmap` – bitmap 对象。

**示例**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

    ┌─res─┐
    │  0  │
    └─────┘

## 位图和 {#bitmapand}

为两个位图对象进行与操作，返回一个新的位图对象。

    bitmapAnd(bitmap1,bitmap2)

**参数**

-   `bitmap1` – 位图对象。
-   `bitmap2` – 位图对象。

**示例**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

    ┌─res─┐
    │ [3] │
    └─────┘

## 位图或 {#bitmapor}

为两个位图对象进行或操作，返回一个新的位图对象。

    bitmapOr(bitmap1,bitmap2)

**参数**

-   `bitmap1` – 位图对象。
-   `bitmap2` – 位图对象。

**示例**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

    ┌─res─────────┐
    │ [1,2,3,4,5] │
    └─────────────┘

## bitmapXor {#bitmapxor}

为两个位图对象进行异或操作，返回一个新的位图对象。

    bitmapXor(bitmap1,bitmap2)

**参数**

-   `bitmap1` – 位图对象。
-   `bitmap2` – 位图对象。

**示例**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

    ┌─res───────┐
    │ [1,2,4,5] │
    └───────────┘

## bitmapAndnot {#bitmapandnot}

计算两个位图的差异，返回一个新的位图对象。

    bitmapAndnot(bitmap1,bitmap2)

**参数**

-   `bitmap1` – 位图对象。
-   `bitmap2` – 位图对象。

**示例**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

    ┌─res───┐
    │ [1,2] │
    └───────┘

## bitmapCardinality {#bitmapcardinality}

返回一个UInt64类型的数值，表示位图对象的基数。

    bitmapCardinality(bitmap)

**参数**

-   `bitmap` – 位图对象。

**示例**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   5 │
    └─────┘

## bitmapMin {#bitmapmin}

返回一个UInt64类型的数值，表示位图中的最小值。如果位图为空则返回UINT32_MAX。

    bitmapMin(bitmap)

**参数**

-   `bitmap` – 位图对象。

**示例**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   1 │
    └─────┘

## bitmapMax {#bitmapmax}

返回一个UInt64类型的数值，表示位图中的最大值。如果位图为空则返回0。

    bitmapMax(bitmap)

**参数**

-   `bitmap` – 位图对象。

**示例**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   5 │
    └─────┘

## 位图和标准性 {#bitmapandcardinality}

为两个位图对象进行与操作，返回结果位图的基数。

    bitmapAndCardinality(bitmap1,bitmap2)

**参数**

-   `bitmap1` – 位图对象。
-   `bitmap2` – 位图对象。

**示例**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

    ┌─res─┐
    │   1 │
    └─────┘

## bitmapOrCardinality {#bitmaporcardinality}

为两个位图进行或运算，返回结果位图的基数。

    bitmapOrCardinality(bitmap1,bitmap2)

**参数**

-   `bitmap1` – 位图对象。
-   `bitmap2` – 位图对象。

**示例**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

    ┌─res─┐
    │   5 │
    └─────┘

## bitmapXorCardinality {#bitmapxorcardinality}

为两个位图进行异或运算，返回结果位图的基数。

    bitmapXorCardinality(bitmap1,bitmap2)

**参数**

-   `bitmap1` – 位图对象。
-   `bitmap2` – 位图对象。

**示例**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

    ┌─res─┐
    │   4 │
    └─────┘

## 位图和非标准性 {#bitmapandnotcardinality}

计算两个位图的差异，返回结果位图的基数。

    bitmapAndnotCardinality(bitmap1,bitmap2)

**参数**

-   `bitmap1` – 位图对象。
-   `bitmap2` - 位图对象。

**示例**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

    ┌─res─┐
    │   2 │
    └─────┘

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/bitmap_functions/) <!--hide-->
