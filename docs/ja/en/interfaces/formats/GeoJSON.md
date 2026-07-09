---
alias: []
description: 'GeoJSON FeatureCollection ドキュメント用の入出力フォーマット。入力時は feature ごとに id、geometry、properties の各カラムを持つ 1 行として扱われ、出力時は 1 行につき 1 つの feature になります。'
input_format: true
output_format: true
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

[GeoJSON](https://geojson.org/) データは、単一の [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3) ドキュメントとしてやり取りされ、ClickHouse ではこれを 3 つのカラム — `id`、`geometry`、`properties` — に対応付けます。各 `Feature` につき 1 組です。ドキュメントを[読み取る](#reading-data)と、`Feature` ごとに 1 行が生成されます。[書き込む](#writing-data)と、1 行ごとに 1 つの `Feature` が生成されます。

<div id="reading-data">
  ## データの読み取り
</div>

`FeatureCollection` を読み込むと、feature ごとに 1 行生成され、次の固定スキーマが適用されます。

| Column       | Type               | Description                                                                                                                            |
| ------------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `id`         | `Nullable(String)` | feature の `id` メンバー (JSON 文字列または数値) です。テキストとして格納され、`id` が存在しないか `null` の場合は `NULL` になります。一方、明示的に空文字列の `id` が指定されている場合は `''` のまま保持されます。 |
| `geometry`   | `Geometry`         | feature のジオメトリです。`Geometry` の バリアント 型として格納されます。                                                                                      |
| `properties` | `Nullable(JSON)`   | feature の `properties` オブジェクトです。半構造化 `JSON` カラムとして格納されます。明示的な `"properties": null` は `NULL` として保持されます。                                 |

各ジオメトリは ClickHouse の `Geometry` 型 (`バリアント`) に格納されます。サポートされる GeoJSON ジオメトリ型 は `Point`、`LineString`、`MultiLineString`、`Polygon`、`MultiPolygon` です。これ以外の 2 つの GeoJSON ジオメトリ型である `GeometryCollection` と `MultiPoint` は `Geometry` 型では表現できません。これらを `geometry` カラムに読み込むと、デフォルトでは例外が発生しますが、代わりに `NULL` を挿入するよう変更することもできます。詳しくは下記の [Handling unsupported geometry types](#unsupported-geometry) を参照してください。デフォルトでは、`geometry` カラムが `NULL` になるのは、feature のジオメトリが明示的な JSON `null` である場合のみです。`input_format_geojson_unsupported_geometry_handling = 'null'` を指定すると、サポートされていないジオメトリ型の場合も `NULL` になります。

ドキュメントの structure は検証されます。最上位の `type` は `FeatureCollection` でなければならず、`features` の各要素は `type` が `Feature` でなければなりません。デフォルトでは、座標は GeoJSON の invariant を満たす必要があります。`LineString` (および `MultiLineString` の各 line) は少なくとも 2 つの Point を持つ必要があり、`Polygon` の ring (および `MultiPolygon` の各 ring) は閉じていて、少なくとも 4 つの Point を持つ必要があります ([Geometry validation](#geometry-validation) を参照) 。不正なドキュメントは黙って読み込まれるのではなく、エラーとして拒否されます。

キーの順序には柔軟性があります。最上位の `type` は `features` 配列の前でも後でもよく、ジオメトリオブジェクト内でも `coordinates` は `type` の前後どちらにあってもかまいません。

スキーマ推論では上記の固定スキーマが返されるため、`DESCRIBE` と `SELECT ... FROM format(...)` はテーブル定義なしで利用できます。

以下の GeoJSON ファイル `london.geojson` には、複数のジオメトリ型が含まれています。

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [-0.0761, 51.5081]},
            "properties": {"name": "Tower of London", "feature_type": "landmark", "year_built": 1078}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {
                "type": "LineString",
                "coordinates": [[-0.2500, 51.4700], [-0.1800, 51.4900], [-0.1200, 51.5060], [-0.0700, 51.5050], [0.0000, 51.5100]]
            },
            "properties": {"name": "River Thames", "feature_type": "river", "length_km": 346}
        },
        {
            "type": "Feature",
            "id": "3",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-0.1880, 51.5074], [-0.1533, 51.5074], [-0.1533, 51.5153], [-0.1880, 51.5153], [-0.1880, 51.5074]]]
            },
            "properties": {"name": "Hyde Park", "feature_type": "park", "area_km2": 1.42}
        }
    ]
}
```

ファイルにクエリを実行し、ジオメトリ型を確認できます：

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
┌─id─┬─name────────────┬─geo_type───┐
│ 1  │ Tower of London │ Point      │
│ 2  │ River Thames    │ LineString │
│ 3  │ Hyde Park       │ Polygon    │
└────┴─────────────────┴────────────┘
```

ファイル拡張子 `.geojson` は自動的に検出されるため、フォーマット引数は省略できます。

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson');
```

各Geometryオブジェクトの内部の型は、`variantType` を使って確認できます：

```sql title="Query"
SELECT properties.name AS name, geometry, variantType(geometry)
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
geometry:              (-0.0761,51.5081)
variantType(geometry): Point

Row 2:
──────
name:                  River Thames
geometry:              [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
variantType(geometry): LineString

Row 3:
──────
name:                  Hyde Park
geometry:              [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
variantType(geometry): Polygon
```

基になるデータは、次のように抽出できます。

```sql title="Query"
SELECT properties.name AS name, variantType(geometry), geometry.Point, geometry.LineString, geometry.Polygon
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
variantType(geometry): Point
geometry.Point:        (-0.0761,51.5081)
geometry.LineString:   []
geometry.Polygon:      []

Row 2:
──────
name:                  River Thames
variantType(geometry): LineString
geometry.Point:        (0,0)
geometry.LineString:   [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
geometry.Polygon:      []

Row 3:
──────
name:                  Hyde Park
variantType(geometry): Polygon
geometry.Point:        (0,0)
geometry.LineString:   []
geometry.Polygon:      [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
```

`Geometry` のサブカラムにアクセスすると、その行にその型が格納されている場合はその値が返され、そうでない場合はその型のデフォルト値が返されます。`Point` なら `(0,0)`、配列ベースの型なら `[]` です。そのため、どの型が設定されているかを判別するには `variantType(geometry)` を使用してください。

GeoJSON データをテーブルに取り込むこともできます。

```sql title="Query"
CREATE TABLE london
(
    id           String,
    geometry     Geometry,
    properties   Nullable(JSON),
    name         String MATERIALIZED properties.name,
    feature_type String MATERIALIZED properties.feature_type
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO london
SELECT id, geometry, properties
FROM file('london.geojson', GeoJSON);
```

次に、地物タイプでクエリを実行します:

```sql title="Query"
SELECT name, feature_type, variantType(geometry) AS geo_type
FROM london
ORDER BY id;
```

```response title="Response"
┌─name────────────┬─feature_type─┬─geo_type───┐
│ Tower of London │ landmark     │ Point      │
│ River Thames    │ river        │ LineString │
│ Hyde Park       │ park         │ Polygon    │
└─────────────────┴──────────────┴────────────┘
```

テーブル定義がなくても、GeoJSONデータのスキーマを推論できます。

```sql title="Query"
DESCRIBE format(GeoJSON, '{"type":"FeatureCollection","features":[]}');
```

```response title="Response"
┌─name───────┬─type─────────────┐
│ id         │ Nullable(String) │
│ geometry   │ Geometry         │
│ properties │ Nullable(JSON)   │
└────────────┴──────────────────┘
```

<div id="unsupported-geometry">
  ### サポートされていないジオメトリ型の処理
</div>

`GeometryCollection` や `MultiPoint` など、一部の有効な GeoJSON のジオメトリ型は ClickHouse の `Geometry` 型では表現できません。こうしたジオメトリを `geometry` カラムに格納する必要がある場合の動作は、`input_format_geojson_unsupported_geometry_handling` 設定で制御できます。設定可能な値は次のとおりです。

* `'throw'` — 例外をスローする (デフォルト) 
* `'null'` — `geometry` カラムに `NULL` 値を挿入し、パースを続行する

この処理が適用されるのは、`geometry` カラムが読み取られる場合に限られます。`geometry` が要求された出力カラムに含まれない場合 (たとえば `SELECT id FROM ...`) 、サポートされていないジオメトリでも、形式が正しいかどうかの検証は行われますが、この処理はトリガーされません。ジオメトリ値はマテリアライズされないため、例外はスローされず、`NULL` も挿入されません。

<div id="reading-limitations">
  ### 制限事項
</div>

読み取り時に反映されるのは固定スキーマに収まる内容だけであるため、GeoJSON の一部の情報は保持されません。

* 生成されるのは `id`、`geometry`、`properties` のみで、その他のドキュメント構造はカラムとしては公開されません。
* 位置の 3 番目の座標 (標高) と、それ以降の座標は破棄されるため、位置は `[longitude, latitude]` になります。
* `bbox` と外部メンバー (トップレベルの `name` や `crs`、あるいは `Feature` 内の追加メンバーなど) は無視されます。
* 数値の `id` はテキストとして格納されるため、文字列と数値の区別は失われます。`id` が存在しないか `null` の場合は `NULL` になります。
* `GeometryCollection` と `MultiPoint` は表現できません。詳しくは [サポートされないジオメトリ型の扱い](#unsupported-geometry) を参照してください。

<div id="writing-data">
  ## データの書き込み
</div>

結果セットを書き込むと、GeoJSON の [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3) が 1 つ生成され、各行が 1 つの `Feature` になります。

結果のカラムは、各 `Feature` に次のようにマッピングされます。

| Feature member | Built from     | Notes                                                                                                                                                                                                      |
| -------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`         | —              | 常に `"Feature"` です。                                                                                                                                                                                         |
| `geometry`     | 単一のジオメトリ型カラム   | ジオメトリ型のカラムは必ず 1 つだけ必要で、そうでない場合はクエリが拒否されます。`NULL` のジオメトリは `null` として書き込まれます。                                                                                                                                |
| `id`           | `id` という名前のカラム | 値が `NULL` の場合は省略されます。`String` カラムは JSON 文字列として、数値カラムは JSON の数値として書き込まれます。                                                                                                                                  |
| `properties`   | 残りのすべてのカラム     | `properties` という名前の単一カラムで、その型が object 系 (`JSON`、`Map`、または名前付き `Tuple`) の場合は、`properties` キーの下にネストせず、`properties` オブジェクトとして直接書き込まれます。それ以外の場合は、残りの各カラムが、そのカラム名をキーとする 1 つのプロパティになります (該当するカラムがない場合は空オブジェクト) 。 |

ジオメトリ型カラムには `Geometry` バリアントまたは特定の Geo 型を使用でき、いずれも GeoJSON のジオメトリ型にマッピングされます。

| ClickHouse type   | GeoJSON `"type"`          |
| ----------------- | ------------------------- |
| `Point`           | `Point`                   |
| `LineString`      | `LineString`              |
| `MultiLineString` | `MultiLineString`         |
| `Polygon`         | `Polygon`                 |
| `MultiPolygon`    | `MultiPolygon`            |
| `Ring`            | `Polygon` (単一リング)         |
| `Geometry`        | 現在有効なバリアントの型 (または `null`) |

`Ring` は GeoJSON のジオメトリ型ではありません。[linear ring](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6) は `Polygon` の構成要素であるため、`Ring` の値は単一リングの `Polygon` として書き込まれます。

<div id="writing-examples">
  ### 例
</div>

上で[作成した](#reading-data) `london` テーブルについて、通常の属性カラムをエクスポートすると、`id` と `geometry` 以外のすべてのカラムはプロパティとして扱われます。

```sql title="Query"
SELECT id, geometry, name, feature_type
FROM london
ORDER BY id
FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London","feature_type":"landmark"}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"name":"River Thames","feature_type":"river"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"name":"Hyde Park","feature_type":"park"}}]}
```

`properties` という名前の単独のオブジェクト型カラムはそのまま直接書き出されるため、GeoJSONファイルを読み込んでそのまま書き戻すと、元のドキュメントが再現されます (このファイルに対して推論されるカラムは `id`、`geometry`、`properties` です) :

```sql title="Query"
SELECT * FROM file('london.geojson', GeoJSON) FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"feature_type":"landmark","name":"Tower of London","year_built":1078}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"feature_type":"river","length_km":346,"name":"River Thames"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"area_km2":1.42,"feature_type":"park","name":"Hyde Park"}}]}
```

数値の`id`カラムは、JSONの数値として書き込まれます (`NULL`である`Nullable`の`id`は完全に省略されます) :

```sql title="Query"
SELECT 42 AS id, (-0.1276, 51.5072)::Point AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":42,"geometry":{"type":"Point","coordinates":[-0.1276,51.5072]},"properties":{}}]}
```

`Ring` は単一リングの `Polygon` として記述されます:

```sql title="Query"
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,0]]]},"properties":{}}]}
```

<div id="writing-to-a-file">
  ### ファイルへの書き込み
</div>

`INTO OUTFILE` を使用して、クライアントから GeoJSON ファイルに書き込みます。

```sql title="Query"
SELECT id, geometry, properties
FROM london
ORDER BY id
INTO OUTFILE 'london_export.geojson'
FORMAT GeoJSON;
```

サーバーは `file` テーブル関数を使って、ファイルに直接書き込むことができます (`.geojson` 拡張子に応じてフォーマットが自動的に選択されます) :

```sql title="Query"
INSERT INTO FUNCTION file('london_export.geojson', GeoJSON)
SELECT id, geometry, properties FROM london;
```

<div id="reading-limitations">
  ### 制限事項
</div>

:::note
ClickHouse の Geo 型には座標参照系が含まれないため、出力では、座標がすでに WGS84 の経度/緯度であり、[RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-4) で規定されている `[longitude, latitude]` の順序になっていることを前提とします。再投影や軸の入れ替えは行われないため、投影座標や `(latitude, longitude)` の形式で格納されたデータは、構造上は有効でも GeoJSON 仕様には準拠しない結果になります。
:::

出力には、ClickHouse に格納されている内容だけが反映されます。

* 読み込み時に失われる情報 — 位置の標高、`bbox`、外部メンバー、`id` が文字列か数値かという区別 — は復元できません。詳しくは [読み込み時の制限事項](#reading-limitations) を参照してください。
* 座標は `Float64` の値から、最短の往復変換可能な表現で書き出されます。
* `JSON` カラムから直接取得した `properties` オブジェクトは、`JSON` 型の canonical なキー順で出力されるため、入力時と順序が異なる場合があります。

ジオメトリは格納されたとおりに書き出され、座標の順序と巻き方向は保持されます。既定では、書き込み時に GeoJSON の形状の妥当性が検証されます ([Geometry validation](#geometry-validation) を参照) 。そのため、1 点しか持たない `LineString` や閉じていない `Polygon` リングのように、有効な GeoJSON 形状ではないジオメトリは、書き出したドキュメントを再度読み込めるようにするため拒否されます。代わりに、そのようなジオメトリをそのまま出力するには、`format_geojson_validate_geometry = 0` を設定してください。この場合、構造上は有効でも GeoJSON 仕様には準拠しない出力になります。右手系の規則 (巻き方向) の invariant は、いずれの場合も強制されず、`null` と空の `properties` オブジェクトの区別も保持されます。

<div id="geometry-validation">
  ## ジオメトリの検証
</div>

設定 `format_geojson_validate_geometry` は、このフォーマットで [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1) のジオメトリ形状ルールを読み書きの両方向で適用するかどうかを制御します。デフォルトでは有効です。

有効な場合、GeoJSON の形状ルールに違反するジオメトリは拒否されます。たとえば、2 点未満の `LineString` (または `MultiLineString` を構成するライン) 、4 点未満の `Polygon` または `MultiPolygon` のリング、先頭と末尾の点が異なるリング (閉じていないリング) 、あるいは空の `MultiLineString`、`Polygon`、`MultiPolygon` です。同じルールは、このようなドキュメントを読み取る場合にも、このような ClickHouse の値を書き出す場合にも適用されるため、書き出したドキュメントは常に再度読み取れます。

無効な場合、これらの形状ルールはどちらの方向でも適用されません。退化したジオメトリは、そのまま読み取られ、そのまま書き出されます。これにより、有効な GeoJSON ジオメトリではない ClickHouse のジオメトリ値でも、このフォーマットを通じて往復変換できますが、その代わりに有効な GeoJSON ではないドキュメントが生成されます。

検証の対象は構造だけです。つまり、点の数とリングが閉じているかどうかだけを確認します。shape の幾何学的な正しさまでは調べないため、構造的には有効でも幾何学的には退化しているジオメトリは、どちらの方向でも受け入れられます。たとえば、面積が 0 の polygon、自己交差するリング、または holes (内側のリング) が外側のリングの外にある polygon などです。polygon のリングの右手則による巻き方向も、同様に適用されることはありません。

1 つのチェックはこの設定とは無関係です。有限でない座標 (`NaN`、`Inf`) は、JSON の数値として表現できないため、常に拒否されます。