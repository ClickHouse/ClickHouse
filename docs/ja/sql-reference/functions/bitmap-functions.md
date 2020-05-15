---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 49
toc_title: Bitmap
---

# ビットマップ関数 {#bitmap-functions}

ビットマップ関数は、二つのビットマップオブジェクトの値の計算のために働く、そのような、および、または、xor、およびない、などの式の計算を使用し

ビットマップオブジェクトの構築方法には2種類あります。 一つは-stateを持つ集約関数groupbitmapによって構築されることであり、もう一つは配列オブジェクトによって構築されることである。 また、bitmapオブジェクトをarrayオブジェクトに変換します。

RoaringBitmapは、ビットマップオブジェクトの実際の格納中にデータ構造にラップされます。 基数が32以下の場合、Set objetが使用されます。 カーディナリティが32より大きい場合、Rooaringbitmapオブジェクトが使用されます。 そのため、低カーディナリティセットの保存が高速になります。

RoaringBitmapの詳細については、以下を参照してください: [鳴き声](https://github.com/RoaringBitmap/CRoaring).

## bitmapBuild {#bitmap_functions-bitmapbuild}

符号なし整数配列からビットマップを作成します。

``` sql
bitmapBuild(array)
```

**パラメータ**

-   `array` – unsigned integer array.

**例えば**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res)
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)    │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray {#bitmaptoarray}

ビットマップを整数配列に変換します。

``` sql
bitmapToArray(bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapSubsetInRange {#bitmap-functions-bitmapsubsetinrange}

指定された範囲のサブセットを返します(range\_endは含みません)。

``` sql
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**パラメータ**

-   `bitmap` – [ビットマップ](#bitmap_functions-bitmapbuild).
-   `range_start` – range start point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `range_end` – range end point(excluded). Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**例えば**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

``` text
┌─res───────────────┐
│ [30,31,32,33,100] │
└───────────────────┘
```

## bitmapSubsetLimit {#bitmapsubsetlimit}

ビットマップのサブセットを作成します。 `range_start` と `cardinality_limit`.

**構文**

``` sql
bitmapSubsetLimit(bitmap, range_start, cardinality_limit)
```

**パラメータ**

-   `bitmap` – [ビットマップ](#bitmap_functions-bitmapbuild).
-   `range_start` – The subset starting point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `cardinality_limit` – The subset cardinality upper limit. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**戻り値**

サブセット。

タイプ: `Bitmap object`.

**例えば**

クエリ:

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res
```

結果:

``` text
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## bitmapContains {#bitmap_functions-bitmapcontains}

かどうかをチェックしますビットマップを含む要素になります。

``` sql
bitmapContains(haystack, needle)
```

**パラメータ**

-   `haystack` – [ビットマップ](#bitmap_functions-bitmapbuild)、関数が検索する場所。
-   `needle` – Value that the function searches. Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   0 — If `haystack` 含まない `needle`.
-   1 — If `haystack` 含む `needle`.

タイプ: `UInt8`.

**例えば**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny {#bitmaphasany}

るかどうかを判二つのビットマップしていることで交差点にある。

``` sql
bitmapHasAny(bitmap1, bitmap2)
```

あなたが確信している場合 `bitmap2` 一つの要素が含まれています。 [bitmapContains](#bitmap_functions-bitmapcontains) 機能。 これは、より効率的に動作します。

**パラメータ**

-   `bitmap*` – bitmap object.

**戻り値**

-   `1`,もし `bitmap1` と `bitmap2` 少なくとも同様の要素を持っている。
-   `0` そうでなければ

**例えば**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll {#bitmaphasall}

に類似した `hasAll(array, array)` 最初のビットマップに1番目のビットマップのすべての要素が含まれる場合は0を返します。
二番目の引数が空のビットマップの場合、1を返します。

``` sql
bitmapHasAll(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  0  │
└─────┘
```

## bitmapCardinality {#bitmapcardinality}

UInt64型のビットマップのカーディナリティを再度実行可能。

``` sql
bitmapCardinality(bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapmincomment {#bitmapmin}

セット内のタイプuint64の最小値を再度取り消し、セットが空の場合はuint32\_max。

    bitmapMin(bitmap)

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   1 │
    └─────┘

## bitmapMax {#bitmapmax}

セット内のuint64型の最大値を取り消し、セットが空の場合は0になります。

    bitmapMax(bitmap)

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   5 │
    └─────┘

## bitmapTransform {#bitmaptransform}

ビットマップ内の値の配列を別の値の配列に変換すると、結果は新しいビットマップになります。

    bitmapTransform(bitmap, from_array, to_array)

**パラメータ**

-   `bitmap` – bitmap object.
-   `from_array` – UInt32 array. For idx in range \[0, from\_array.size()), if bitmap contains from\_array\[idx\], then replace it with to\_array\[idx\]. Note that the result depends on array ordering if there are common elements between from\_array and to\_array.
-   `to_array` – UInt32 array, its size shall be the same to from\_array.

**例えば**

``` sql
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res
```

    ┌─res───────────────────┐
    │ [1,3,4,6,7,8,9,10,20] │
    └───────────────────────┘

## bitmapAnd {#bitmapand}

二つのビットマップと計算、結果は新しいビットマップです。

``` sql
bitmapAnd(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr {#bitmapor}

二つのビットマップや計算、結果は新しいビットマップです。

``` sql
bitmapOr(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor {#bitmapxor}

二つのビットマップxor計算、結果は新しいビットマップです。

``` sql
bitmapXor(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot {#bitmapandnot}

二つのビットマップと計算ではなく、結果は新しいビットマップです。

``` sql
bitmapAndnot(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapAndCardinality {#bitmapandcardinality}

二つのビットマップと計算、型uint64の戻り値のカーディナリティ。

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## ビットmapcardinality {#bitmaporcardinality}

二つのビットマップまたは計算、型uint64の戻り値のカーディナリティ。

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality {#bitmapxorcardinality}

二つのビットマップxor計算、型uint64の戻り値のカーディナリティ。

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality {#bitmapandnotcardinality}

二つのビットマップと計算ではなく、型uint64のカーディナリティを返します。

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例えば**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   2 │
└─────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/bitmap_functions/) <!--hide-->
