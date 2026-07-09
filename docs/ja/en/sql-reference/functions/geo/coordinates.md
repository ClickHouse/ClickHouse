---
description: '地理座標に関するドキュメント'
sidebar_label: '地理座標'
slug: /sql-reference/functions/geo/coordinates
title: '地理座標を扱う関数'
doc_type: 'reference'
---

<div id="greatcircledistance">
  ## greatCircleDistance
</div>

[大円距離の公式](https://en.wikipedia.org/wiki/Great-circle_distance)を使用して、地球表面上の2点間の距離を計算します。

```sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**入力パラメータ**

* `lon1Deg` — 1つ目の Point の経度 (度) 。範囲: `[-180°, 180°]`。
* `lat1Deg` — 1つ目の Point の緯度 (度) 。範囲: `[-90°, 90°]`。
* `lon2Deg` — 2つ目の Point の経度 (度) 。範囲: `[-180°, 180°]`。
* `lat2Deg` — 2つ目の Point の緯度 (度) 。範囲: `[-90°, 90°]`。

正の値は北緯および東経、負の値は南緯および西経を表します。

**戻り値**

地球表面上の 2 点間の距離 (メートル単位) 。

入力パラメータの値が範囲外の場合、例外をスローします。

**例**

```sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673) AS greatCircleDistance
```

```text
┌─greatCircleDistance─┐
│            14128352 │
└─────────────────────┘
```

<div id="geodistance">
  ## geoDistance
</div>

`greatCircleDistance` と似ていますが、球面ではなく WGS-84 楕円体上での距離を計算します。これは地球ジオイドをより正確に近似したものです。
パフォーマンスは `greatCircleDistance` と同等です (性能上のデメリットはありません) 。地球上の距離を計算する場合は、`geoDistance` を使用することを推奨します。

技術的な注記: 十分に近い点については、座標の中点における接平面上の計量を用いた平面近似で距離を計算します。

```sql
geoDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**入力パラメータ**

* `lon1Deg` — 1 つ目の Point の経度 (度) 。範囲: `[-180°, 180°]`。
* `lat1Deg` — 1 つ目の Point の緯度 (度) 。範囲: `[-90°, 90°]`。
* `lon2Deg` — 2 つ目の Point の経度 (度) 。範囲: `[-180°, 180°]`。
* `lat2Deg` — 2 つ目の Point の緯度 (度) 。範囲: `[-90°, 90°]`。

正の値は北緯および東経、負の値は南緯および西経に対応します。

**戻り値**

地球表面上の 2 点間の距離 (メートル単位) 。

入力パラメータの値が範囲外の場合は例外が発生します。

**例**

```sql
SELECT geoDistance(38.8976, -77.0366, 39.9496, -75.1503) AS geoDistance
```

```text
┌─geoDistance─┐
│   212458.73 │
└─────────────┘
```

<div id="greatcircleangle">
  ## greatCircleAngle
</div>

[大円距離の公式](https://en.wikipedia.org/wiki/Great-circle_distance)を用いて、地球表面上の2点間の中心角を計算します。

```sql
greatCircleAngle(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**入力パラメータ**

* `lon1Deg` — 1つ目の点の経度 (度) 。
* `lat1Deg` — 1つ目の点の緯度 (度) 。
* `lon2Deg` — 2つ目の点の経度 (度) 。
* `lat2Deg` — 2つ目の点の緯度 (度) 。

**戻り値**

2点間の中心角 (度) 。

**例**

```sql
SELECT greatCircleAngle(0, 0, 45, 0) AS arc
```

```text
┌─arc─┐
│  45 │
└─────┘
```

<div id="geotoutm">
  ## geoToUTM
</div>

WGS84の地理座標 `(longitude, latitude)` を [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) 座標に変換します。

UTMは60個の横メルカトル投影法からなる座標系で、それぞれが経度6°幅のゾーンをカバーし、地理座標をメートル単位の平面グリッドに写像します。明示的な `zone` が指定されていない場合、ゾーンは経度に基づいて自動的に選択され、ノルウェーおよびスヴァールバル諸島に関する標準的な例外が適用されます。UTMが定義されているのは緯度 `[-80°, 84°]` の範囲のみで、極域では別個のUPSシステムが使用されます。

```sql
geoToUTM(longitude, latitude[, zone])
```

**引数**

* `longitude` — 度単位の経度。範囲: `[-180°, 180°]`。[`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md)。
* `latitude` — 度単位の緯度。範囲: `[-80°, 84°]`。[`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md)。
* `zone` — 任意。自動選択する代わりに、この UTM ゾーンを使用するように投影先を固定します。範囲: `[1, 60]`。[`(U)Int*`](../../data-types/int-uint.md)。

**戻り値**

名前付きタプル `(easting, northing, zone, band)` を返します。`easting` と `northing` はメートル単位 ([`Float64`](../../data-types/float.md)) 、UTM の `zone` 番号 ([`UInt8`](../../data-types/int-uint.md)) 、`band` は MGRS の緯度帯を表す文字 ([`FixedString(1)`](../../data-types/fixedstring.md)) です。`band` が `'N'` 以降であれば北半球を示します。

緯度が `[-80°, 84°]` の範囲外、または経度が `[-180°, 180°]` の範囲外の場合は、例外が発生します。

**例**

```sql
SELECT geoToUTM(2.294497, 48.858222) AS utm; -- Eiffel Tower
```

```text
(448251.5978370684,5411935.125629659,31,'U')
```

<div id="utmtogeo">
  ## UTMToGeo
</div>

[UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) 座標を WGS84 の地理座標 `(longitude, latitude)` に変換し直します。これは [`geoToUTM`](#geotoutm) の逆変換です。

```sql
UTMToGeo(easting, northing, zone, is_north)
```

**引数**

* `easting` — メートル単位の東距 (500000 m の仮東距を含む) 。[`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md)。
* `northing` — メートル単位の北距 (南半球では 10000000 m の仮北距を含む) 。[`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md)。
* `zone` — UTM ゾーン番号。範囲: `[1, 60]`。[`(U)Int*`](../../data-types/int-uint.md)。
* `is_north` — 半球: 北半球は `1`、南半球は `0`。[`(U)Int*`](../../data-types/int-uint.md)。

**戻り値**

度単位の名前付きタプル `(longitude, latitude)`。[`Tuple(Float64, Float64)`](../../data-types/tuple.md)。

**例**

```sql
SELECT UTMToGeo(448251.6, 5411935.13, 31, 1) AS coord;
```

```text
(2.2944970289079203,48.85822204127082)
```

<div id="geotomgrs">
  ## geoToMGRS
</div>

WGS84 の地理座標 `(longitude, latitude)` を [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) 文字列にエンコードします。

文字列の形式は `<zone><band><100km square><easting><northing>` で、たとえば `31UDQ4825111935` です。`precision` 引数は、easting と northing のそれぞれに使用する桁数を指定します。`5` (既定値) は 1 m、`4` は 10 m、`3` は 100 m、`2` は 1 km、`1` は 10 km、`0` は 100 km グリッド square のみを表します。MGRS は、緯度が `[-80°, 84°]` の範囲内でのみ定義されています。

```sql
geoToMGRS(longitude, latitude[, precision])
```

**引数**

* `longitude` — 度単位の経度。範囲: `[-180°, 180°]`。[`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md)。
* `latitude` — 度単位の緯度。範囲: `[-80°, 84°]`。[`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md)。
* `precision` — 省略可能。easting と northing のそれぞれの桁数。デフォルト: `5`。範囲: `[0, 5]`。[`(U)Int*`](../../data-types/int-uint.md)。

**戻り値**

MGRS 参照文字列。[`String`](../../data-types/string.md)。

**例**

```sql
SELECT geoToMGRS(2.294497, 48.858222) AS mgrs, geoToMGRS(2.294497, 48.858222, 3) AS mgrs_100m;
```

```text
┌─mgrs────────────┬─mgrs_100m───┐
│ 31UDQ4825111935 │ 31UDQ482119 │
└─────────────────┴─────────────┘
```

<div id="mgrstogeo">
  ## MGRSToGeo
</div>

[MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) 文字列を WGS84 の地理座標 `(longitude, latitude)` にデコードします。これは [`geoToMGRS`](#geotomgrs) の逆変換です。

返される Point は参照先のグリッド正方形の中心点であるため、結果の精度は文字列にエンコードされた精度と一致します。入力中の空白は無視され、英字の大文字と小文字は区別されません。

```sql
MGRSToGeo(mgrs)
```

**引数**

* `mgrs` — デコードする MGRS参照文字列。[`String`](../../data-types/string.md)/[`FixedString`](../../data-types/fixedstring.md)。

**戻り値**

度単位の名前付きタプル `(longitude, latitude)` です。[`Tuple(Float64, Float64)`](../../data-types/tuple.md)。

**例**

```sql
SELECT MGRSToGeo('31UDQ4825111935') AS coord;
```

```text
(2.294495618908297,48.85822536113692)
```

<div id="pointinellipses">
  ## pointInEllipses
</div>

Pointが少なくとも1つの楕円内に含まれるかどうかを判定します。
座標はデカルト座標系における幾何学的な座標です。

```sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**入力パラメータ**

* `x, y` — 平面上の点の座標。
* `xᵢ, yᵢ` — `i` 番目の楕円の中心座標。
* `aᵢ, bᵢ` — `i` 番目の楕円の軸の長さ (`x`、`y` 座標の単位) 。

入力パラメータの総数は `2+4⋅n` である必要があります。ここで、`n` は楕円の数です。

**戻り値**

点が少なくとも 1 つの楕円の内側にある場合は `1`、そうでない場合は `0`。

**例**

```sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

```text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

<div id="pointinpolygon">
  ## pointInPolygon
</div>

平面上で、点が多角形内にあるかどうかを判定します。

```sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**入力値**

* `(x, y)` — 平面上の Point の座標。データ型 — [Tuple](../../data-types/tuple.md) — 2 つの数値からなるタプル。
* `[(a, b), (c, d) ...]` — Polygon の頂点。データ型 — [Array](../../data-types/array.md)。各頂点は座標の組 `(a, b)` で表されます。頂点は時計回りまたは反時計回りの順序で指定する必要があります。頂点の最小数は 3 です。Polygon は定数である必要があります。
* この関数は holes (切り抜かれた領域) を持つ Polygon もサポートします。データ型 — [Polygon](../../data-types/geo.md/#polygon)。`Polygon` 全体を第 2 引数として渡すか、最初に外側の ring を渡し、その後に各 hole を個別の追加引数として渡します。
* この関数は multipolygon もサポートします。データ型 — [MultiPolygon](../../data-types/geo.md/#multipolygon)。`MultiPolygon` 全体を第 2 引数として渡すか、構成する各 polygon をそれぞれ個別の引数として指定します。

**戻り値**

Point が Polygon の内側にある場合は `1`、ない場合は `0` を返します。
Point が Polygon の境界上にある場合、関数は `0` または `1` のいずれかを返すことがあります。

**例**

```sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```text
┌─res─┐
│   1 │
└─────┘
```

> **注記**
> • `validate_polygons = 0` を設定すると、ジオメトリの検証をスキップできます。
> • `pointInPolygon` は、すべてのポリゴンが適切な形式であることを前提としています。入力が自己交差している、リングの順序が誤っている、または辺が重なっている場合、結果は信頼できなくなります。特に、点が辺上や頂点上にちょうどある場合や、&quot;内側&quot; と &quot;外側&quot; の区別が未定義な自己交差部分の内部にある場合に顕著です。
> • ポリゴン引数が定数で、点が索引付きキーカラムを使って表現されている場合 (たとえば、`x, y` が `PRIMARY KEY` の一部である、または `minmax` 索引でカバーされているテーブルに対する `pointInPolygon((x, y), constant_polygon)` など) 、ClickHouse は主キーと `minmax` データスキッピングインデックスの両方を使って、不要なグラニュールを除外できます。