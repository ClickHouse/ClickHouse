---
description: 'Geohash に関するドキュメント'
sidebar_label: 'Geohash'
slug: /sql-reference/functions/geo/geohash
title: 'Geohash を扱う関数'
doc_type: 'reference'
---

<div id="geohash">
  ## Geohash
</div>

[Geohash](https://en.wikipedia.org/wiki/Geohash) は、地球の表面をグリッド状の区画に分割し、各セルを文字と数字からなる短い文字列に符号化するジオコード方式です。階層的なデータ構造になっているため、geohash 文字列が長いほど、地理的位置の精度は高くなります。

地理座標を geohash 文字列に手動で変換する必要がある場合は、[geohash.org](http://geohash.co/) を使用できます。

<div id="geohashencode">
  ## geohashEncode
</div>

緯度と経度を [geohash](#geohash) 文字列にエンコードします。

**構文**

```sql
geohashEncode(longitude, latitude, [precision])
```

**入力値**

* `longitude` — エンコードする座標の経度。`[-180°, 180°]` の範囲の浮動小数点数。[Float](../../data-types/float.md)。
* `latitude` — エンコードする座標の緯度。`[-90°, 90°]` の範囲の浮動小数点数。[Float](../../data-types/float.md)。
* `精度` (省略可) — エンコード後の文字列の長さ。既定値は `12` です。範囲 `[1, 12]` の整数。[Int8](../../data-types/int-uint.md)。

:::note

* すべての座標パラメータは同じ型である必要があります。`Float32` または `Float64` のいずれかです。
* `精度` パラメータでは、`1` 未満または `12` を超える値は、警告なしで `12` に変換されます。
  :::

**戻り値**

* エンコードされた座標を表す英数字の文字列 (base32 エンコーディングのアルファベットを修正したものを使用) 。[String](../../data-types/string.md)。

**例**

```sql title="Query"
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

```text title="Response"
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

<div id="geohashdecode">
  ## geohashDecode
</div>

[geohash](#geohash) でエンコードされた任意の文字列を、経度と緯度にデコードします。

**構文**

```sql
geohashDecode(hash_str)
```

**入力値**

* `hash_str` — Geohash で符号化された文字列。

**戻り値**

* 経度と緯度を表す `Float64` 型の値からなる Tuple `(longitude, latitude)`。[Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md))

**例**

```sql
SELECT geohashDecode('ezs42') AS res;
```

```text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

<div id="geohashesinbox">
  ## geohashesInBox
</div>

指定されたボックスの内部または境界にかかる、指定した精度の[geohash](#geohash)エンコード文字列の配列を返します。これは基本的に、2 次元グリッドを配列にフラット化したものです。

**構文**

```sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**引数**

* `longitude_min` — 最小経度。範囲: `[-180°, 180°]`。[Float](../../data-types/float.md)。
* `latitude_min` — 最小緯度。範囲: `[-90°, 90°]`。[Float](../../data-types/float.md)。
* `longitude_max` — 最大経度。範囲: `[-180°, 180°]`。[Float](../../data-types/float.md)。
* `latitude_max` — 最大緯度。範囲: `[-90°, 90°]`。[Float](../../data-types/float.md)。
* `precision` — geohash の精度。範囲: `[1, 12]`。[UInt8](../../data-types/int-uint.md)。

:::note
すべての座標パラメーターは同じ型、つまり `Float32` または `Float64` のいずれかでなければなりません。
:::

**戻り値**

* 指定した領域をカバーする geohash ボックスを表す、長さが precision の文字列の Array。要素の順序に依存しないでください。[Array](../../data-types/array.md)([String](../../data-types/string.md))。
* `[]` - 最小緯度および最小経度の値が、それぞれ対応する最大値より小さくない場合は空の配列を返します。

:::note
結果の配列が 10&#39;000&#39;000 要素を超える場合、この関数は例外をスローします。
:::

**例**

```sql title="Query"
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos;
```

```text title="Response"
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```