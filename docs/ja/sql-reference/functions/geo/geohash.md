---
slug: /ja/sql-reference/functions/geo/geohash
sidebar_label: Geohash
title: "Geohashを扱うための関数"
---

## Geohash

[Geohash](https://en.wikipedia.org/wiki/Geohash)は、地球の表面を格子状のバケツに分割し、各セルを短い文字と数字の文字列でエンコードするジオコードシステムです。これは階層的なデータ構造であり、geohash文字列が長ければ長いほど、より正確な地理的位置を表します。

地理座標を手動でgeohash文字列に変換する必要がある場合は、[geohash.org](http://geohash.org/)を使用できます。

## geohashEncode

緯度と経度を[geohash](#geohash)文字列としてエンコードします。

**構文**

``` sql
geohashEncode(longitude, latitude, [precision])
```

**入力値**

- `longitude` — エンコードしたい座標の経度部分。範囲`[-180°, 180°]`の浮動小数点。[Float](../../data-types/float.md)。
- `latitude` — エンコードしたい座標の緯度部分。範囲 `[-90°, 90°]`の浮動小数点。[Float](../../data-types/float.md)。
- `precision` (オプション) — 結果として得られるエンコードされた文字列の長さ。デフォルトは`12`。整数で範囲 `[1, 12]`。[Int8](../../data-types/int-uint.md)。

:::note
- すべての座標パラメータは同じ型である必要があります: `Float32`または`Float64`。
- `precision`パラメータでは、`1`未満または`12`を超える値は黙って`12`に変換されます。
:::

**返される値**

- エンコードされた座標の英数字文字列（base32エンコーディングアルファベットの修正版が使用されます）。[String](../../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

結果:

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode

任意の[geohash](#geohash)エンコーディングされた文字列を経度と緯度にデコードします。

**構文**

```sql
geohashDecode(hash_str)
```

**入力値**

- `hash_str` — Geohashエンコードされた文字列。

**返される値**

- 経度と緯度の`Float64`値のタプル`(longitude, latitude)`。[Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md))

**例**

``` sql
SELECT geohashDecode('ezs42') AS res;
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geohashesInBox

指定された精度の[geohash](#geohash)エンコードされた文字列の配列を返します。この配列に含まれる文字列は、指定されたボックスの境界内に存在し、交差します。基本的には2Dグリッドを配列に平坦化したものです。

**構文**

``` sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**引数**

- `longitude_min` — 最小経度。範囲: `[-180°, 180°]`。[Float](../../data-types/float.md)。
- `latitude_min` — 最小緯度。範囲: `[-90°, 90°]`。[Float](../../data-types/float.md)。
- `longitude_max` — 最大経度。範囲: `[-180°, 180°]`。[Float](../../data-types/float.md)。
- `latitude_max` — 最大緯度。範囲: `[-90°, 90°]`。[Float](../../data-types/float.md)。
- `precision` — Geohashの精度。範囲: `[1, 12]`。[UInt8](../../data-types/int-uint.md)。

:::note    
すべての座標パラメータは同じ型である必要があります: `Float32`または`Float64`。
:::

**返される値**

- 指定されたエリアをカバーする geohashボックスの配列で、その順序に依存するべきではありません。[Array](../../data-types/array.md)([String](../../data-types/string.md))。
- `[]` - 最小の緯度および経度の値が、それぞれの最大値を下回らない場合は空の配列。

:::note    
関数は、結果の配列が10,000,000項目を超えると例外を投げます。
:::

**例**

クエリ:

``` sql
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos;
```

結果:

``` text
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```
