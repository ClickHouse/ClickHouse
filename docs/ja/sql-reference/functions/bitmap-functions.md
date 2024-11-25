---
slug: /ja/sql-reference/functions/bitmap-functions
sidebar_position: 25
sidebar_label: Bitmap
---

# Bitmap関数

ビットマップは2つの方法で構築できます。1つ目は集計関数groupBitmapと`-State`を用いて構築する方法で、もう1つは配列オブジェクトからビットマップを構築する方法です。

## bitmapBuild

符号なし整数配列からビットマップを構築します。

**構文**

``` sql
bitmapBuild(array)
```

**引数**

- `array` – 符号なし整数配列。

**例**

``` sql
SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res);
```

``` text
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)        │
└─────┴──────────────────────────────────────────────┘
```

## bitmapToArray

ビットマップを整数配列に変換します。

**構文**

``` sql
bitmapToArray(bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

結果:

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapSubsetInRange

値の範囲内のビットを持つビットマップのサブセットを返します。

**構文**

``` sql
bitmapSubsetInRange(bitmap, range_start, range_end)
```

**引数**

- `bitmap` – [ビットマップオブジェクト](#bitmapbuild)。
- `range_start` – 範囲の開始点（含む）。[UInt32](../data-types/int-uint.md)。
- `range_end` – 範囲の終了点（排他的）。[UInt32](../data-types/int-uint.md)。

**例**

``` sql
SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res;
```

結果:

``` text
┌─res───────────────┐
│ [30,31,32,33,100] │
└───────────────────┘
```

## bitmapSubsetLimit

最小のビット値`range_start`を持ち、最大`cardinality_limit`個の要素を持つビットマップのサブセットを返します。

**構文**

``` sql
bitmapSubsetLimit(bitmap, range_start, cardinality_limit)
```

**引数**

- `bitmap` – [ビットマップオブジェクト](#bitmapbuild)。
- `range_start` – 範囲の開始点（含む）。[UInt32](../data-types/int-uint.md)。
- `cardinality_limit` – サブセットの最大カーディナリティ。[UInt32](../data-types/int-uint.md)。

**例**

``` sql
SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(30), toUInt32(200))) AS res;
```

結果:

``` text
┌─res───────────────────────┐
│ [30,31,32,33,100,200,500] │
└───────────────────────────┘
```

## subBitmap

指定した位置`offset`から始まるビットマップのサブセットを返します。返されるビットマップの最大カーディナリティは`cardinality_limit`です。

**構文**

``` sql
subBitmap(bitmap, offset, cardinality_limit)
```

**引数**

- `bitmap` – ビットマップ。[ビットマップオブジェクト](#bitmapbuild)。
- `offset` – サブセットの最初の要素の位置。[UInt32](../data-types/int-uint.md)。
- `cardinality_limit` – サブセット内の要素の最大数。[UInt32](../data-types/int-uint.md)。

**例**

``` sql
SELECT bitmapToArray(subBitmap(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toUInt32(10), toUInt32(10))) AS res;
```

結果:

``` text
┌─res─────────────────────────────┐
│ [10,11,12,13,14,15,16,17,18,19] │
└─────────────────────────────────┘
```

## bitmapContains

ビットマップに要素が含まれているか確認します。

``` sql
bitmapContains(bitmap, needle)
```

**引数**

- `bitmap` – [ビットマップオブジェクト](#bitmapbuild)。
- `needle` – 検索するビット値。[UInt32](../data-types/int-uint.md)。

**返される値**

- 0 — `bitmap`が`needle`を含まない場合。[UInt8](../data-types/int-uint.md)。
- 1 — `bitmap`が`needle`を含む場合。[UInt8](../data-types/int-uint.md)。

**例**

``` sql
SELECT bitmapContains(bitmapBuild([1,5,7,9]), toUInt32(9)) AS res;
```

結果:

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAny

2つのビットマップが交差しているか確認します。

`bitmap2`が正確に1つの要素を含む場合、[bitmapContains](#bitmapcontains)を使用することを検討してみてください。より効率的に動作します。

**構文**

``` sql
bitmapHasAny(bitmap1, bitmap2)
```

**引数**

- `bitmap1` – ビットマップオブジェクト1。
- `bitmap2` – ビットマップオブジェクト2。

**返される値**

- `1` - `bitmap1`と`bitmap2`が少なくとも1つの共有要素を持つ場合。
- `0` - それ以外の場合。

**例**

``` sql
SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

結果:

``` text
┌─res─┐
│  1  │
└─────┘
```

## bitmapHasAll

最初のビットマップが2番目のビットマップのすべての要素を含む場合は1を返し、そうでない場合は0を返します。 2番目のビットマップが空であれば、1を返します。

`hasAll(array, array)`も参照してください。

**構文**

``` sql
bitmapHasAll(bitmap1, bitmap2)
```

**引数**

- `bitmap1` – ビットマップオブジェクト1。
- `bitmap2` – ビットマップオブジェクト2。

**例**

``` sql
SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

結果:

``` text
┌─res─┐
│  0  │
└─────┘
```

## bitmapCardinality

ビットマップのカーディナリティを返します。

**構文**

``` sql
bitmapCardinality(bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

結果:

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapMin

ビットマップで設定されている最小のビットを算出し、ビットマップが空の場合はUINT32_MAXを返します。

**構文**

```sql 
bitmapMin(bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

結果:

``` text
 ┌─res─┐
 │   1 │
 └─────┘
```

## bitmapMax

ビットマップで設定されている最大のビットを算出し、ビットマップが空の場合は0を返します。

**構文**

```sql 
bitmapMax(bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res;
```

結果:

``` text
 ┌─res─┐
 │   5 │
 └─────┘
```

## bitmapTransform

ビットマップ内のビットを最大N個置き換えます。置き換えられるビットの古い値と新しい値は、それぞれ`from_array[i]`と`to_array[i]`で指定されます。

`from_array`と`to_array`の配列の順序によって結果が異なります。

**構文**

``` sql
bitmapTransform(bitmap, from_array, to_array)
```

**引数**

- `bitmap` – ビットマップオブジェクト。
- `from_array` – UInt32配列。範囲\[0, from_array.size())内のidxについて、ビットマップがfrom_array\[idx\]を含む場合、それをto_array\[idx\]で置き換えます。
- `to_array` – `from_array`と同じサイズのUInt32配列。

**例**

``` sql
SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res;
```

結果:

``` text
 ┌─res───────────────────┐
 │ [1,3,4,6,7,8,9,10,20] │
 └───────────────────────┘
```

## bitmapAnd

2つのビットマップの論理積を計算します。

**構文**

``` sql
bitmapAnd(bitmap,bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

結果:

``` text
┌─res─┐
│ [3] │
└─────┘
```

## bitmapOr

2つのビットマップの論理和を計算します。

**構文**

``` sql
bitmapOr(bitmap,bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

結果:

``` text
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
```

## bitmapXor

2つのビットマップの排他的論理和を計算します。

**構文**

``` sql
bitmapXor(bitmap,bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

結果:

``` text
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
```

## bitmapAndnot

2つのビットマップの論理積を計算して結果を否定します。

**構文**

``` sql
bitmapAndnot(bitmap,bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;
```

結果:

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## bitmapAndCardinality

2つのビットマップの論理積のカーディナリティを返します。

**構文**

``` sql
bitmapAndCardinality(bitmap,bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

結果:

``` text
┌─res─┐
│   1 │
└─────┘
```

## bitmapOrCardinality

2つのビットマップの論理和のカーディナリティを返します。

``` sql
bitmapOrCardinality(bitmap,bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

結果:

``` text
┌─res─┐
│   5 │
└─────┘
```

## bitmapXorCardinality

2つのビットマップの排他的論理和のカーディナリティを返します。

``` sql
bitmapXorCardinality(bitmap,bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

結果:

``` text
┌─res─┐
│   4 │
└─────┘
```

## bitmapAndnotCardinality

2つのビットマップのAND-NOT操作のカーディナリティを返します。

``` sql
bitmapAndnotCardinality(bitmap,bitmap)
```

**引数**

- `bitmap` – ビットマップオブジェクト。

**例**

``` sql
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;
```

結果:

``` text
┌─res─┐
│   2 │
└─────┘
```

