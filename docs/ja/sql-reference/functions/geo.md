---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "\u5730\u7406\u5EA7\u6A19\u306E\u64CD\u4F5C"
---

# 地理座標を操作するための関数 {#functions-for-working-with-geographical-coordinates}

## グレートサークル距離 {#greatcircledistance}

を使用して、地球の表面上の二つの点の間の距離を計算します [大円の公式](https://en.wikipedia.org/wiki/Great-circle_distance).

``` sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**入力パラメータ**

-   `lon1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]`.
-   `lat1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]`.
-   `lon2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]`.
-   `lat2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]`.

正の値は北緯と東経に対応し、負の値は南緯と西経に対応します。

**戻り値**

メートル単位で、地球の表面上の二つの点の間の距離。

入力パラメーター値が範囲外にある場合に例外を生成します。

**例**

``` sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

``` text
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## ポイントネリップス {#pointinellipses}

点が少なくとも一方の楕円に属しているかどうかをチェックします。
座標はデカルト座標系では幾何学的です。

``` sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**入力パラメータ**

-   `x, y` — Coordinates of a point on the plane.
-   `xᵢ, yᵢ` — Coordinates of the center of the `i`-番目の省略記号。
-   `aᵢ, bᵢ` — Axes of the `i`-x、y座標の単位で番目の省略記号。

入力パラメータ `2+4⋅n`,ここで `n` 楕円の数です。

**戻り値**

`1` 点が楕円の少なくとも一方の内側にある場合; `0`そうでない場合。

**例**

``` sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

``` text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## ポイントポリゴン {#pointinpolygon}

ポイントが平面上のポリゴンに属するかどうかを確認します。

``` sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**入力値**

-   `(x, y)` — Coordinates of a point on the plane. Data type — [タプル](../../sql-reference/data-types/tuple.md) — A tuple of two numbers.
-   `[(a, b), (c, d) ...]` — Polygon vertices. Data type — [配列](../../sql-reference/data-types/array.md). 各頂点は、座標のペアで表されます `(a, b)`. 頂点は時計回りまたは反時計回りの順序で指定する必要があります。 頂点の最小数は3です。 多角形は一定でなければなりません。
-   この機能にも対応し多角形穴あき(切り抜く部門). この場合、関数の追加引数を使用して切り取られたセクションを定義するポリゴンを追加します。 この機能はサポートしない非単に接続ポリゴン.

**戻り値**

`1` ポイントがポリゴンの内側にある場合, `0` そうでない場合。
ポイントがポリゴン境界上にある場合、関数は0または1のいずれかを返します。

**例**

``` sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## geohashEncode {#geohashencode}

緯度と経度をジオハッシュ文字列としてエンコードします。http://geohash.org/,https://en.wikipedia.org/wiki/Geohash）。

``` sql
geohashEncode(longitude, latitude, [precision])
```

**入力値**

-   経度-エンコードする座標の経度部分。 範囲の浮遊`[-180°, 180°]`
-   緯度-エンコードする座標の緯度の部分。 範囲の浮遊 `[-90°, 90°]`
-   精度-オプションで、結果のエンコードされた文字列の長さ。 `12`. 範囲内の整数 `[1, 12]`. より小さい値 `1` またはより大きい `12` に変換されます。 `12`.

**戻り値**

-   英数字 `String` 符号化座標（base32エンコーディングアルファベットの修正バージョンが使用されます）。

**例**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res
```

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode {#geohashdecode}

ジオハッシュでエンコードされた文字列を経度と緯度にデコードします。

**入力値**

-   エンコード文字列-ジオハッシュエンコード文字列。

**戻り値**

-   (経度,緯度)-2-タプルの `Float64` 経度と緯度の値。

**例**

``` sql
SELECT geohashDecode('ezs42') AS res
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geoToH3 {#geotoh3}

ﾂづｩﾂ。 [H3](https://uber.github.io/h3/#/documentation/overview/introduction) ポイント指数 `(lon, lat)` 指定決断を使って。

[H3](https://uber.github.io/h3/#/documentation/overview/introduction) 地球の表面が六角形のタイルに分割された地理的索引システムです。 このシステムは階層的であり、すなわち最上位の各六角形は七つに分割することができますが、より小さなものなどです。

このインデックスは、主にバケットの場所やその他の地理空間操作に使用されます。

**構文**

``` sql
geoToH3(lon, lat, resolution)
```

**パラメータ**

-   `lon` — Longitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `lat` — Latitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. タイプ: [UInt8](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   六角形のインデックス番号。
-   エラーの場合は0。

タイプ: `UInt64`.

**例**

クエリ:

``` sql
SELECT geoToH3(37.79506683, 55.71290588, 15) as h3Index
```

結果:

``` text
┌────────────h3Index─┐
│ 644325524701193974 │
└────────────────────┘
```

## geohashesInBox {#geohashesinbox}

指定されたボックスの内側にあり、指定されたボックスの境界と交差する、指定された精度のジオハッシュエンコードされた文字列の配列を返しま

**入力値**

-   longitudie_min-最小の経度、範囲内の浮動小数点値 `[-180°, 180°]`
-   latitude_min-最小緯度、範囲内の浮動小数点値 `[-90°, 90°]`
-   縦方向max-最大経度、範囲内の浮動小数点値 `[-180°, 180°]`
-   latitude_max-最大緯度、範囲内の浮動小数点値 `[-90°, 90°]`
-   精度-ジオハッシュ精度, `UInt8` 範囲内 `[1, 12]`

すべての座標パラメータは同じタイプでなければなりません。 `Float32` または `Float64`.

**戻り値**

-   精度の配列-提供された領域をカバーするジオハッシュボックスの長い文字列、あなたは項目の順序に頼るべきではありません。
-   \[\]-空の配列の場合 *分* の値 *緯度* と *経度* 対応するよりも小さくない *最大* 値。

結果の配列が10'000'000項目以上の場合、関数は例外をスローすることに注意してください。

**例**

``` sql
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos
```

``` text
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```

## h3GetBaseCell {#h3getbasecell}

インデックスの基本セル番号を返します。

**構文**

``` sql
h3GetBaseCell(index)
```

**パラメータ**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   六角形のベースのセル番号。 タイプ: [UInt8](../../sql-reference/data-types/int-uint.md).

**例**

クエリ:

``` sql
SELECT h3GetBaseCell(612916788725809151) as basecell
```

結果:

``` text
┌─basecell─┐
│       12 │
└──────────┘
```

## h3HexAreaM2 {#h3hexaream2}

与えられた解像度での平方メートルの平均六角形面積。

**構文**

``` sql
h3HexAreaM2(resolution)
```

**パラメータ**

-   `resolution` — Index resolution. Range: `[0, 15]`. タイプ: [UInt8](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   Area in m². Type: [Float64](../../sql-reference/data-types/float.md).

**例**

クエリ:

``` sql
SELECT h3HexAreaM2(13) as area
```

結果:

``` text
┌─area─┐
│ 43.9 │
└──────┘
```

## h3IndexesAreNeighbors {#h3indexesareneighbors}

指定されたH3indexがneighborであるかどうかを返します。

**構文**

``` sql
h3IndexesAreNeighbors(index1, index2)
```

**パラメータ**

-   `index1` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `index2` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   ﾂづｩﾂ。 `1` インデックスが近傍の場合, `0` そうでなければ タイプ: [UInt8](../../sql-reference/data-types/int-uint.md).

**例**

クエリ:

``` sql
SELECT h3IndexesAreNeighbors(617420388351344639, 617420388352655359) AS n
```

結果:

``` text
┌─n─┐
│ 1 │
└───┘
```

## h3ToChildren {#h3tochildren}

指定したインデックスの子インデックスを持つ配列を返します。

**構文**

``` sql
h3ToChildren(index, resolution)
```

**パラメータ**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. タイプ: [UInt8](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   子h3インデックスを持つ配列。 型の配列: [UInt64](../../sql-reference/data-types/int-uint.md).

**例**

クエリ:

``` sql
SELECT h3ToChildren(599405990164561919, 6) AS children
```

結果:

``` text
┌─children───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [603909588852408319,603909588986626047,603909589120843775,603909589255061503,603909589389279231,603909589523496959,603909589657714687] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## h3ToParent {#h3toparent}

指定された索引を含む親(より粗い)索引を返します。

**構文**

``` sql
h3ToParent(index, resolution)
```

**パラメータ**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. タイプ: [UInt8](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   親H3インデックス。 タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

**例**

クエリ:

``` sql
SELECT h3ToParent(599405990164561919, 3) as parent
```

結果:

``` text
┌─────────────parent─┐
│ 590398848891879423 │
└────────────────────┘
```

## h3ToString {#h3tostring}

インデックスのH3Index表現を文字列表現に変換します。

``` sql
h3ToString(index)
```

**パラメータ**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   H3インデックスの文字列表現。 タイプ: [文字列](../../sql-reference/data-types/string.md).

**例**

クエリ:

``` sql
SELECT h3ToString(617420388352917503) as h3_string
```

結果:

``` text
┌─h3_string───────┐
│ 89184926cdbffff │
└─────────────────┘
```

## stringToH3 {#stringtoh3}

文字列表現をH3Index(UInt64)表現に変換します。

``` sql
stringToH3(index_str)
```

**パラメータ**

-   `index_str` — String representation of the H3 index. Type: [文字列](../../sql-reference/data-types/string.md).

**戻り値**

-   六角形のインデックス番号。 エラーの場合は0を返します。 タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

**例**

クエリ:

``` sql
SELECT stringToH3('89184926cc3ffff') as index
```

結果:

``` text
┌──────────────index─┐
│ 617420388351344639 │
└────────────────────┘
```

## h3GetResolution {#h3getresolution}

インデックスの解像度を返します。

**構文**

``` sql
h3GetResolution(index)
```

**パラメータ**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**戻り値**

-   インデックスの解決。 範囲: `[0, 15]`. タイプ: [UInt8](../../sql-reference/data-types/int-uint.md).

**例**

クエリ:

``` sql
SELECT h3GetResolution(617420388352917503) as res
```

結果:

``` text
┌─res─┐
│   9 │
└─────┘
```

[元の記事](https://clickhouse.com/docs/en/query_language/functions/geo/) <!--hide-->
