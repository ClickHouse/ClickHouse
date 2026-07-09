---
description: 'Mapbox Vector Tiles のエンコードに関するドキュメント'
sidebar_label: 'Mapbox Vector Tiles'
sidebar_position: 65
slug: /sql-reference/functions/geo/mvt
title: 'Mapbox Vector Tiles をエンコードする関数'
doc_type: 'reference'
---

<div id="overview">
  ## 概要
</div>

[Mapbox Vector Tiles](https://github.com/mapbox/vector-tile-spec) (MVT) は、MapLibre や Mapbox GL などの Web マップ
クライアントがネイティブにレンダリングする、Protobuf エンコードされたタイルです。ClickHouse では、このようなタイルを SQL だけで、連携して動作する 2 つの
関数によって構築できます。

* `MVTEncodeGeom` — ジオメトリを slippy-map タイルのタイル内ローカルなピクセル空間に投影し、
  タイルに合わせてクリップするスカラー関数です。
* `MVTEncode` — グループ内の投影済みジオメトリを集約し、単一レイヤーのタイルのバイナリバイト列に変換する
  集約関数です。

2 つの補助関数 `MVTBoundingBox` と `MVTBoundingBoxMercator` はタイルのバウンディングボックスを返すため、行を
`WHERE` 句で索引を使ってその範囲内に絞り込めます。

Point、line、polygon のジオメトリをサポートしており、`Geometry` 型と具体的な geo types (`Point`、
`LineString`、`MultiLineString`、`Ring`、`Polygon`、`MultiPolygon`) も含まれます。

結果として得られるバイト列は完全なタイルであり、`FORMAT RawBLOB` を使って HTTP インターフェイス経由で直接返せます。

これらの関数は PostGIS のワークフローを踏襲しており、PostGIS での名前の別名としても利用できます。`MVTEncodeGeom` の別名は `ST_AsMVTGeom`、
`MVTEncode` の別名は `ST_AsMVT` です。

<div id="mvtencodegeom">
  ## MVTEncodeGeom
</div>

地理座標 (経度/緯度) で与えられたジオメトリを、`zoom`、`tile_x`、`tile_y` で特定される
slippy-map タイルのタイルローカルなピクセル空間に投影し、整数ピクセルグリッドにスナップしてタイルにクリップし、
タイル空間のジオメトリを返します。

この投影は、`UInt32` の座標範囲全体に対する Web Mercator です。返される座標の始点はタイルの
左上隅で、y 軸は下向きです。これは Mapbox Vector
Tile フォーマットの座標規約であるため、結果はそのまま `MVTEncode` に渡せます。座標は整数ピクセルに丸められるため、
`MVTEncodeGeom` でグループ化すると、同じグリッド上にあるジオメトリは 1 つのクラスターにまとめられます。

`clip` が有効な場合 (デフォルト) 、ジオメトリは `buffer` ピクセル分だけ拡張されたタイル (各軸で
`[-buffer, extent + buffer]` の範囲) にクリップされます。完全に外側にあるジオメトリは `NULL` になります。これは
PostGIS の `ST_AsMVTGeom` に相当します。

Polygon の座標は、検証前に `2^30` の範囲に制限されます。これは `zoom` 18、`extent` 4096 における
世界全体のピクセル幅とちょうど一致します。そのため、現実的なタイルではジオメトリは検証されてもクリップされることはなく、この制限が
影響するのは `zoom` または `extent` が極端な値の場合に配置されたジオメトリだけです。

出力されるジオメトリ型は入力に依存します。`Point` は `Point` を返し、`LineString` または `MultiLineString` は
`MultiLineString` を返します。`Ring`、`Polygon`、または `MultiPolygon` は `MultiPolygon` を返します (クリップによって 1 つのジオメトリが
複数のパーツに分割されることがあります) 。

**構文**

```sql
MVTEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]])
```

**引数**

* `geometry` — 経度/緯度 (度単位) で表したジオメトリ。経度は `[-180, 180]` に、緯度は Web Mercator の範囲 `[-85.05112878, 85.05112878]` に制限されます。[`Point`](../../data-types/geo.md) / [`LineString`](../../data-types/geo.md) / [`MultiLineString`](../../data-types/geo.md) / [`Ring`](../../data-types/geo.md) / [`Polygon`](../../data-types/geo.md) / [`MultiPolygon`](../../data-types/geo.md) / [`Geometry`](../../data-types/geo.md)。
* `zoom` — Slippy-map のズームレベル。範囲は `[0, 32]` です。[`UInt8`](../../data-types/int-uint.md)。
* `tile_x` — タイルの列インデックス。範囲は `[0, 2^zoom - 1]` です。[`UInt32`](../../data-types/int-uint.md)。
* `tile_y` — タイルの行インデックス。範囲は `[0, 2^zoom - 1]` です。[`UInt32`](../../data-types/int-uint.md)。
* `extent` — 省略可能なタイルの extent で、1 辺あたりのピクセル数で指定します。範囲は `[1, 2147483647]` です。既定値は `4096` で、Mapbox Vector Tile の既定値です。[`UInt32`](../../data-types/int-uint.md)。
* `buffer` — 省略可能なクリップバッファ (ピクセル単位) 。範囲は `[0, 2147483647]` です。既定値は `1` です。[`UInt32`](../../data-types/int-uint.md)。
* `clip` — 省略可能なフラグ。0 以外 (既定値) の場合、ジオメトリはタイルとバッファを含む範囲にクリップされます。[`UInt8`](../../data-types/int-uint.md)。

**戻り値**

タイル空間のジオメトリを返します。完全にクリップされて除外される場合は `NULL` を返します。[`Geometry`](../../data-types/geo.md)。

**例**

```sql
SELECT MVTEncodeGeom((13.37, 52.52)::Point, 10, 550, 335) AS pixel
```

```text
┌─pixel──────┐
│ (124,3384) │
└────────────┘
```

<div id="mvtencode">
  ## MVTEncode
</div>

地物のグループをバイナリの Mapbox Vector Tile レイヤーにエンコードします。これはスカラー関数 `MVTEncodeGeom` に対応する集約版です。各入力行は 1 つのフィーチャとなり、Point、line、Polygon ジオメトリをサポートします。

`geometry` argument はタイル空間座標の `Geometry` で、通常は `MVTEncodeGeom` によって生成されます。`geometry` が `NULL` の行 (たとえば `MVTEncodeGeom` によってクリップされて除外されたもの) はスキップされます。省略可能な `properties` argument は named tuple であり、その要素名は地物の属性キーとなり、要素の型によってベクタータイルの値の型が決まります。

結果は単一レイヤータイルの生のバイト列です。空のグループからは空のタイルが生成されます。これは PostGIS `ST_AsMVT` に相当します。

**構文**

```sql
MVTEncode(layer_name[, extent[, feature_id_name[, stringify_unsupported]]])(geometry[, properties])
```

**パラメータ**

* `layer_name` — ベクタータイルレイヤーの名前。[`String`](../../data-types/string.md)。
* `extent` — タイルの1辺あたりのピクセル数で表した範囲。`[1, 2147483647]` の範囲を指定できます。デフォルトは `4096` です。[`UInt32`](../../data-types/int-uint.md)。
* `feature_id_name` — タグではなく MVT Feature の `id` (`UInt64`) として出力する、`properties` タプル内の符号なし整数要素の名前を指定する省略可能なパラメータです。符号付き整数は受け付けられません。`id` が `NULL` の場合、そのフィーチャでは `id` は省略されます。パラメータは位置指定のため、これを使うには `extent` を指定する必要があります。[`String`](../../data-types/string.md)。
* `stringify_unsupported` — 省略可能なフラグ (`0`/`1`、デフォルトは `0`) です。`1` を指定すると、直接サポートされていないプロパティ型 (たとえば大きな整数、`UUID`、`Decimal`) は、エラーにする代わりにテキストの `string_value` としてエンコードされます。[`UInt8`](../../data-types/int-uint.md)。

**引数**

* `geometry` — タイル空間のジオメトリ。たとえば `MVTEncodeGeom` の出力です。[`Geometry`](../../data-types/geo.md)。
* `properties` — フィーチャ属性を表す省略可能な名前付きタプルです。要素名は属性キーになります。[`Tuple`](../../data-types/tuple.md)。

**戻り値**

単一レイヤーの Mapbox Vector Tile のバイナリ内容を返します。[`String`](../../data-types/string.md)。

<div id="property-types">
  ### プロパティの型
</div>

各プロパティ要素は、ClickHouse の型に対応する Mapbox Vector Tile の `Value` バリアントとしてエンコードされます。

| ClickHouse type                                                | Vector tile value type |
| -------------------------------------------------------------- | ---------------------- |
| `String` / `FixedString`                                       | `string_value`         |
| `Float32` / `BFloat16`                                         | `float_value`          |
| `Float64`                                                      | `double_value`         |
| `Bool`                                                         | `bool_value`           |
| `Int8` / `Int16` / `Int32` / `Int64` / `Date32`                | `sint_value`           |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` / `Date` / `DateTime` | `uint_value`           |

型は `Nullable` や `LowCardinality`、あるいはその両方でラップできます。`NULL` 値の場合、その属性はフィーチャから省略されます。これは
vector tile フォーマットが null をサポートしていないためです。その他のプロパティ型は、`stringify_unsupported` が設定されていない限り例外になります。設定されている場合は、
テキストの `string_value` としてエンコードされます。

同一のプロパティ値はレイヤーの共有値プールにインターンされるため、多くのフィーチャに現れる値でも
1 回だけ保存されます。

<div id="naming-the-properties-tuple">
  ### プロパティのタプルに名前を付ける
</div>

プロパティのタプルでは、要素名を明示的に指定する必要があります。`tuple(...)` 内のカラムの別名はタプルの
要素名には**引き継がれない**ため、要素には CAST を使って名前を付けてください。

```sql
tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)
```

<div id="clustering">
  ### クラスタリング
</div>

クラスタリングは関数ではなく SQL で表現します。`MVTEncodeGeom` は座標をピクセル単位の整数に丸めるため、ピクセルジオメトリでグループ化すると、重なったジオメトリがマージされます。グループはサブクエリで集約し、その後、クラスターごとに 1 行を `MVTEncode` に渡します。

```sql
SELECT MVTEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)) AS tile
FROM
(
    SELECT MVTEncodeGeom((lon, lat)::Point, 10, 550, 335) AS geom, count() AS cluster_count
    FROM points
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1;
```

`Geometry` 値でグループ化するには、`Variant` ベースの
`Geometry` 型のグループ化はデフォルトで制限されているため、`allow_suspicious_types_in_group_by = 1` が必要です。クラスター化されたフィーチャではなく、入力 `行` ごとに 1 つのフィーチャを出力するには、内側の `GROUP BY` (および `count()`) を省略します。

<div id="mvtboundingbox">
  ## MVTBoundingBox
</div>

`zoom`、`tile_x`、`tile_y` で識別される slippy-map タイルの地理的なバウンディングボックスを、度単位のタプル
`(min_lon, min_lat, max_lon, max_lat)` として返します。

各行ごとに Web Mercator 投影を再計算する代わりに、`longitude`/`latitude` カラムに対して直接フィルタしながら、対象の行をタイル内に絞り込むために使用します。これにより、それらのカラムに対する主キーや
索引を利用できます。オプションの `margin` は、タイルサイズに対するその割合だけ、ボックスを各辺で拡張します。`MVTEncodeGeom` のクリップバッファをカバーするには、これを `buffer / extent` に設定します。

**構文**

```sql
MVTBoundingBox(zoom, tile_x, tile_y[, margin])
```

**引数**

* `zoom` — Slippy-map のズームレベル。範囲は `[0, 32]` です。[`UInt8`](../../data-types/int-uint.md)。
* `tile_x` — タイルの列インデックス。範囲は `[0, 2^zoom - 1]` です。[`UInt32`](../../data-types/int-uint.md)。
* `tile_y` — タイルの行インデックス。範囲は `[0, 2^zoom - 1]` です。[`UInt32`](../../data-types/int-uint.md)。
* `margin` — ボックスを各辺で外側に拡張するための、タイルサイズに対する省略可能な割合です。既定値は `0` です。[`Float64`](../../data-types/float.md)。

**戻り値**

タイルのバウンディングボックスを、度単位の `(min_lon, min_lat, max_lon, max_lat)` 形式のタプルとして返します。[`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md)。

**例**

```sql
SELECT MVTBoundingBox(0, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────────────────────────┐
│ (-180,-85.05112877980659,180,85.05112877980659)  │
└──────────────────────────────────────────────────┘
```

<div id="mvtboundingboxmercator">
  ## MVTBoundingBoxMercator
</div>

`MVTBoundingBox` に対応する Web Mercator 版です。`MVTEncodeGeom` が内部で使用する、full-`UInt32` の Web Mercator 座標空間におけるタイルの
バウンディングボックスを、タプル
`(min_x, min_y, max_x, max_y)` として返します。y 軸は下方向に増加します (上が北) 。`longitude`/`latitude` の代わりに Mercator 座標のカラムをマテリアライズし、
それらに索引を設定するテーブルでの利用を想定しています。

**構文**

```sql
MVTBoundingBoxMercator(zoom, tile_x, tile_y[, margin])
```

**引数**

[`MVTBoundingBox`](#mvtboundingbox) と同じです。

**戻り値**

タイルのバウンディングボックスを、Web Mercator 座標系のタプル `(min_x, min_y, max_x, max_y)` として返します。[`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md)。

**例**

```sql
SELECT MVTBoundingBoxMercator(1, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────┐
│ (0,0,2147483648,2147483648)  │
└──────────────────────────────┘
```

<div id="restricting-rows-to-a-tile">
  ## 行をタイルに制限する
</div>

タイルには、そのタイルに属するジオメトリだけを含める必要があります。これを最も適切に表すには、連携して機能する 2 つの手順に分けます。すなわち、`WHERE` 句に記述する、索引を使う低コストなバウンディングボックス条件 (性能) と、`MVTEncodeGeom` のクリップ (正確性) です。
クリップはタイル外のジオメトリを除外するため、バウンディングボックス条件が多少大まかでも、タイル外のジオメトリが結果に
紛れ込むことはありません。

```sql
WITH
    1 AS buffer,
    4096 AS extent,
    MVTBoundingBox({z:UInt8}, {x:UInt32}, {y:UInt32}, buffer / extent) AS bounding_box   -- margin matches the clip buffer
SELECT MVTEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64))
FROM
(
    SELECT MVTEncodeGeom((lon, lat)::Point, {z:UInt8}, {x:UInt32}, {y:UInt32}) AS geom, count() AS cluster_count
    FROM points
    WHERE lon BETWEEN bounding_box.1 AND bounding_box.3 AND lat BETWEEN bounding_box.2 AND bounding_box.4   -- index-using prefilter
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1
```

バウンディングボックスの述語は、あくまで大まかな事前フィルターにすぎません。正確なタイル境界は
`MVTEncodeGeom` の clip によって適用されます。クリッピングを無効にして
`WHERE` 述語のみに依存するには、`clip => false` (7 番目の引数) を `MVTEncodeGeom` に渡します。

<div id="serving-tiles-over-http">
  ## HTTP 経由でのタイル配信
</div>

ClickHouse はデフォルトではタイル用エンドポイントを公開しておらず、HTTP インターフェイスがクエリを受け付けるのは `/` のみです。すっきりした
`/tile/{z}/{x}/{y}` URL は、オペレーターがサーバー設定の [事前定義クエリハンドラー](/ja/interfaces/http) で追加します。ハンドラーの `url` は、`regex:` 形式でパスセグメントをキャプチャし、それらをクエリパラメータにバインドして、`FORMAT RawBLOB` でバイト列を返します。

最も単純なケースでは、テーブルに `Geometry` カラムがあり、ハンドラーは 1 行につき 1 つのフィーチャを返します。`MVTEncodeGeom`
は各ジオメトリを要求されたタイルに投影してクリップするため、タイル外の行は自動的に除外されます。

```xml
<http_handlers>
    <rule>
        <methods>GET</methods>
        <url><![CDATA[regex:/tile/(?P<z>\d+)/(?P<x>\d+)/(?P<y>\d+)]]></url>
        <handler>
            <type>predefined_query_handler</type>
            <query>
                SELECT MVTEncode('shapes')(
                    MVTEncodeGeom(geom, {z:UInt8}, {x:UInt32}, {y:UInt32}),
                    tuple(id, name)::Tuple(id UInt32, name String))
                FROM shapes
                FORMAT RawBLOB
            </query>
            <content_type>application/vnd.mapbox-vector-tile</content_type>
        </handler>
    </rule>
    <defaults/>
</http_handlers>
```

ここで `shapes` は、`geom Geometry` カラム (ポイント、ライン、ポリゴンを任意に混在可能) を持つテーブルです。`GET /tile/10/550/335`
は、エンコード済みのタイルを返します。

ポイントデータの場合も、`MVTEncodeGeom((lon, lat)::Point, …)` で Point をその場で構築することで、
通常の `longitude`/`latitude` カラムに対して同様に機能します。一致するフィーチャをクラスタリングする場合や、大きなテーブルに対して索引を使うバウンディングボックスの事前フィルタ
を追加する場合は、[Clustering](#clustering) および
[Restricting rows to a tile](#restricting-rows-to-a-tile) に示すように内部クエリを拡張してください。

<div id="limitations">
  ## 制限事項
</div>

* Webメルカトル図法では、緯度は `±85.05112878°` までに制限されており、反経線をまたぐ入力はサポートされていません。