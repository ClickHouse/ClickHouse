---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: "\u30D3\u30C3\u30C8\u30DE"
---

# ビットマップ関数 {#bitmap-functions}

And、or、xor、notなどの数式の計算を使用しながら、新しいビットマップまたは基数を返すことです。

ビットマップオブジェクトの構築方法には2種類あります。 一つは-Stateを持つ集計関数groupBitmapによって構築され、もう一つはArray Objectによって構築されます。 でも変換するビットマップオブジェクト配列のオブジェクトです。

RoaringBitmapは、ビットマップオブジェクトの実際の格納中にデータ構造にラップされます。 基数が32以下の場合、Set objetが使用されます。 基数が32より大きい場合は、RoaringBitmapオブジェクトを使用します。 そのため、低基数セットの保存が高速になります。

RoaringBitmapの詳細については、以下を参照してください: [クロアリング](https://github.com/RoaringBitmap/CRoaring).

## bitmapBuild {#bitmap_functions-bitmapbuild}

符号なし整数配列からビットマップを作成します。

``` sql
bitmapBuild(array)
```

**パラメータ**

-   `array` – unsigned integer array.

**例**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res)
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)        │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray {#bitmaptoarray}

ビットマップを整数配列に変換します。

``` sql
bitmapToArray(bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapSubsetInRange {#bitmap-functions-bitmapsubsetinrange}

指定された範囲のサブセットを返します(range_endは含まれません)。

``` sql
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**パラメータ**

-   `bitmap` – [ビットマップ](#bitmap_functions-bitmapbuild).
-   `range_start` – range start point. Type: [UInt32](../../sql-reference/data-types/int-uint.md).
-   `range_end` – range end point(excluded). Type: [UInt32](../../sql-reference/data-types/int-uint.md).

**例**

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

**例**

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

**例**

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

あなたがそれを確信している場合 `bitmap2` 厳密に一つの要素が含まれています。 [bitmapContains](#bitmap_functions-bitmapcontains) 機能。 これは、より効率的に動作します。

**パラメータ**

-   `bitmap*` – bitmap object.

**戻り値**

-   `1`,if `bitmap1` と `bitmap2` 少なくとも一つの同様の要素があります。
-   `0` そうでなければ

**例**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll {#bitmaphasall}

類似する `hasAll(array, array)` 返り値が1の場合は最初のビットマップを含むすべての要素は、0です。
二番目の引数が空のビットマップの場合は、1を返します。

``` sql
bitmapHasAll(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res
```

``` text
┌─res─┐
│  0  │
└─────┘
```

## bitmapCardinality {#bitmapcardinality}

UInt64型のビットマップ基数を再実行します。

``` sql
bitmapCardinality(bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapMin {#bitmapmin}

セット内のUInt64型の最小値を再実行し、セットが空の場合はUINT32_MAX。

    bitmapMin(bitmap)

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res
```

    ┌─res─┐
    │   1 │
    └─────┘

## bitmapMax {#bitmapmax}

セット内のUInt64型の最大値を再試行し、セットが空の場合は0を再試行します。

    bitmapMax(bitmap)

**パラメータ**

-   `bitmap` – bitmap object.

**例**

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
-   `from_array` – UInt32 array. For idx in range \[0, from_array.size()), if bitmap contains from_array\[idx\], then replace it with to_array\[idx\]. Note that the result depends on array ordering if there are common elements between from_array and to_array.
-   `to_array` – UInt32 array, its size shall be the same to from_array.

**例**

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

**例**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr {#bitmapor}

結果は新しいビットマップです。

``` sql
bitmapOr(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor {#bitmapxor}

二つのビットマップxorの計算は、結果は新しいビットマップです。

``` sql
bitmapXor(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot {#bitmapandnot}

二つのビットマップandnot計算、結果は新しいビットマップです。

``` sql
bitmapAndnot(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapAndCardinality {#bitmapandcardinality}

Uint64型の基数を返します。

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality {#bitmaporcardinality}

Uint64型の基数を返します。

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality {#bitmapxorcardinality}

Uint64型の基数を返します。

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality {#bitmapandnotcardinality}

Uint64型の基数を返します。

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**パラメータ**

-   `bitmap` – bitmap object.

**例**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

``` text
┌─res─┐
│   2 │
└─────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/bitmap_functions/) <!--hide-->
