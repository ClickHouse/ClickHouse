---
slug: /ja/sql-reference/aggregate-functions/reference/largestTriangleThreeBuckets
sidebar_position: 159
sidebar_label: largestTriangleThreeBuckets
---

# largestTriangleThreeBuckets

入力データに[Largest-Triangle-Three-Buckets](https://skemman.is/bitstream/1946/15343/3/SS_MSthesis.pdf)アルゴリズムを適用します。
このアルゴリズムは、時系列データの可視化のためのダウンサンプリングに使用されます。x座標でソートされた系列に対して動作するように設計されています。
ソートされた系列をバケットに分割し、各バケットで最も大きな三角形を見つけることで動作します。バケットの数は、結果のシリーズに含まれるポイントの数に等しいです。
この関数は、データを`x`でソートし、その後ソートされたデータにダウンサンプリングアルゴリズムを適用します。

**構文**

``` sql
largestTriangleThreeBuckets(n)(x, y)
```

エイリアス: `lttb`.

**引数**

- `x` — x座標。 [Integer](../../../sql-reference/data-types/int-uint.md)、[Float](../../../sql-reference/data-types/float.md)、[Decimal](../../../sql-reference/data-types/decimal.md)、[Date](../../../sql-reference/data-types/date.md)、[Date32](../../../sql-reference/data-types/date32.md)、[DateTime](../../../sql-reference/data-types/datetime.md)、[DateTime64](../../../sql-reference/data-types/datetime64.md)。
- `y` — y座標。 [Integer](../../../sql-reference/data-types/int-uint.md)、[Float](../../../sql-reference/data-types/float.md)、[Decimal](../../../sql-reference/data-types/decimal.md)、[Date](../../../sql-reference/data-types/date.md)、[Date32](../../../sql-reference/data-types/date32.md)、[DateTime](../../../sql-reference/data-types/datetime.md)、[DateTime64](../../../sql-reference/data-types/datetime64.md)。

NaNは提供される系列から無視されます。つまり、NaN値は分析から除外されます。これにより、関数が有効な数値データのみに対して動作することが保証されます。

**パラメータ**

- `n` — 結果の系列に含まれるポイント数。 [UInt64](../../../sql-reference/data-types/int-uint.md)。

**返される値**

[Tuple](../../../sql-reference/data-types/tuple.md)と2つの要素を持つ[Array](../../../sql-reference/data-types/array.md)。

**例**

入力テーブル:

``` text
┌─────x───────┬───────y──────┐
│ 1.000000000 │ 10.000000000 │
│ 2.000000000 │ 20.000000000 │
│ 3.000000000 │ 15.000000000 │
│ 8.000000000 │ 60.000000000 │
│ 9.000000000 │ 55.000000000 │
│ 10.00000000 │ 70.000000000 │
│ 4.000000000 │ 30.000000000 │
│ 5.000000000 │ 40.000000000 │
│ 6.000000000 │ 35.000000000 │
│ 7.000000000 │ 50.000000000 │
└─────────────┴──────────────┘
```

クエリ:

``` sql
SELECT largestTriangleThreeBuckets(4)(x, y) FROM largestTriangleThreeBuckets_test;
```

結果:

``` text
┌────────largestTriangleThreeBuckets(4)(x, y)───────────┐
│           [(1,10),(3,15),(9,55),(10,70)]              │
└───────────────────────────────────────────────────────┘
```
