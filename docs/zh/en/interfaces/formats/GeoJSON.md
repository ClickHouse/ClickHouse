---
alias: []
description: 'GeoJSON FeatureCollection 文档的输入/输出格式：输入时，每个要素占一行，包含 id、geometry 和 properties 列；输出时，每行一个要素。'
input_format: true
output_format: true
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

[GeoJSON](https://geojson.org/) 数据以单个 [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3) 文档的形式进行交换，ClickHouse 会将其映射为三个列——`id`、`geometry` 和 `properties`——每个 `Feature` 对应一组。[读取](#reading-data) 文档时，每个 `Feature` 会生成一行；[写入](#writing-data) 时，每行会生成一个 `Feature`。

<div id="reading-data">
  ## 读取数据
</div>

读取 `FeatureCollection` 时，会为每个 feature 生成一行，并采用以下固定 schema：

| Column       | Type               | Description                                                                                   |
| ------------ | ------------------ | --------------------------------------------------------------------------------------------- |
| `id`         | `Nullable(String)` | feature 的 `id` 成员 (JSON 字符串或数值) 会以文本形式存储；如果 `id` 不存在或为 `null`，则为 `NULL`；显式的空字符串 id 则保留为 `''`。 |
| `geometry`   | `Geometry`         | feature 的几何数据，存储为 `Geometry` Variant 类型。                                                      |
| `properties` | `Nullable(JSON)`   | feature 的 `properties` 对象，存储为半结构化的 `JSON` 列。显式的 `"properties": null` 会保留为 `NULL`。             |

每个几何对象都存储在 ClickHouse 的 `Geometry` 类型中 (即 `Variant`) 。支持的几何类型包括 `Point`、`LineString`、`MultiLineString`、`Polygon` 和 `MultiPolygon`。另外两种几何类型——`GeometryCollection` 和 `MultiPoint`——无法由 `Geometry` 类型表示；默认情况下，将其读入 `geometry` 列会引发异常，但也可以改为插入 `NULL`——请参见下方的[处理不受支持的几何类型](#unsupported-geometry)。默认情况下，只有当 feature 的几何数据是显式的 JSON `null` 时，`geometry` 列才为 `NULL`；在 `input_format_geojson_unsupported_geometry_handling = 'null'` 下，对于不受支持的几何类型，它也会是 `NULL`。

系统会对文档的结构进行校验：顶层 `type` 必须为 `FeatureCollection`，并且 `features` 中的每个元素都必须具有 `type` `Feature`。默认情况下，坐标必须满足 GeoJSON 的形态不变式——`LineString` (以及 `MultiLineString` 中的每一条 line) 必须至少包含两个点，而 `Polygon` 的 ring (以及 `MultiPolygon` 中的每个 ring) 必须闭合且至少包含四个点 (参见[几何校验](#geometry-validation)) 。格式错误的文档会被拒绝，而不会被静默加载。

键的顺序比较灵活：顶层 `type` 可以出现在 `features` 数组之前或之后，而在几何对象内部，`coordinates` 也可以出现在 `type` 之前或之后。

Schema inference 会返回上述固定 schema，因此 `DESCRIBE` 和 `SELECT ... FROM format(...)` 无需表定义即可工作。

给定以下 GeoJSON 文件 `london.geojson`，其中包含多种几何类型：

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

我们可以查询该文件并查看几何类型：

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

系统会自动识别 `.geojson` 文件扩展名，因此可以省略 `format` 参数：

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson');
```

我们可以使用 `variantType` 来查看每个 Geometry 对象的底层类型：

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

接着，我们可以这样提取底层数据：

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

访问 `Geometry` 子列时，如果该行保存的是该类型，就返回对应的值；否则返回该类型的默认值——`Point` 为 `(0,0)`，基于数组的类型为 `[]`——因此请使用 `variantType(geometry)` 来判断当前是哪一种类型。

我们也可以将 GeoJSON 数据摄取到表中：

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

然后按要素类型查询：

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

无需表定义，也可以推断 GeoJSON 数据的 schema：

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
  ### 处理不受支持的几何类型
</div>

某些有效的 GeoJSON 几何类型 — 例如 `GeometryCollection` 和 `MultiPoint` — 无法由 ClickHouse 的 `Geometry` 类型表示。你可以使用 `input_format_geojson_unsupported_geometry_handling` 设置来控制当这类几何对象必须存储在 `geometry` 列中时的处理方式。可能的值包括：

* `'throw'` — 抛出异常 (默认) 
* `'null'` — 为 `geometry` 列插入 `NULL` 值并继续解析

这种处理仅在读取 `geometry` 列时适用。当 `geometry` 不是请求的输出列时 (例如 `SELECT id FROM ...`) ，不受支持的几何对象仍会验证其格式是否正确，但不会触发该处理——既不会抛出异常，也不会插入 `NULL`，因为不会将任何几何值 materialize。

<div id="reading-limitations">
  ### 局限性
</div>

读取时只能反映符合固定 schema 的内容，因此部分 GeoJSON 信息不会被保留：

* 只会生成 `id`、`geometry` 和 `properties`；其他文档结构不会作为列暴露。
* 位置的第三个坐标 (高程) 及之后的任何坐标都会被丢弃——位置会变为 `[longitude, latitude]`。
* `bbox` 和外来成员 (例如顶层的 `name` 或 `crs`，或 `Feature` 内部的额外成员) 都会被忽略。
* 数值类型的 `id` 会以文本形式存储，因此字符串与数字的区别会丢失；缺失或为 `null` 的 `id` 会变为 `NULL`。
* `GeometryCollection` 和 `MultiPoint` 无法表示——请参见[处理不受支持的几何类型](#unsupported-geometry)。

<div id="writing-data">
  ## 写入数据
</div>

写入结果集时，会生成一个 GeoJSON [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3)，其中每一行对应一个 `Feature`。

结果中的列会按如下方式映射到每个 `Feature`：

| Feature member | Built from                       | Notes                                                                                                                                                   |
| -------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`         | —                                | 始终为 `"Feature"`。                                                                                                                                        |
| `geometry`     | the single geometry-typed column | 必须且只能有一个几何类型列，否则查询会被拒绝。`NULL` 几何值会写成 `null`。                                                                                                            |
| `id`           | a column named `id`              | 值为 `NULL` 时会省略。`String` 列会写成 JSON 字符串，数值列会写成 JSON 数值。                                                                                                   |
| `properties`   | all remaining columns            | 如果存在一个名为 `properties` 的列，且其类型为对象类型 (`JSON`、`Map` 或具名 `Tuple`) ，则会直接写为 `properties` 对象，而不是再嵌套在 `properties` 键下。否则，其余每一列都会成为一个属性，属性名为列名 (如果没有其余列，则为空对象) 。 |

几何类型列可以是 `Geometry` Variant，也可以是某种具体的地理类型；它们与 GeoJSON 几何类型的映射如下：

| ClickHouse type   | GeoJSON `"type"`    |
| ----------------- | ------------------- |
| `Point`           | `Point`             |
| `LineString`      | `LineString`        |
| `MultiLineString` | `MultiLineString`   |
| `Polygon`         | `Polygon`           |
| `MultiPolygon`    | `MultiPolygon`      |
| `Ring`            | `Polygon` (单个环)     |
| `Geometry`        | 当前变体的类型 (或 `null`)  |

`Ring` 不是 GeoJSON 几何类型——[linear ring](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6) 是 `Polygon` 的一个组成部分——因此，`Ring` 值会写成单环 `Polygon`。

<div id="writing-examples">
  ### 示例
</div>

继续以上文[创建](#reading-data)的 `london` 表为例，导出普通属性列会将除 `id` 和 `geometry` 之外的每一列都转为一个属性：

```sql title="Query"
SELECT id, geometry, name, feature_type
FROM london
ORDER BY id
FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London","feature_type":"landmark"}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"name":"River Thames","feature_type":"river"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"name":"Hyde Park","feature_type":"park"}}]}
```

由于名为 `properties` 的唯一对象类型列会被直接写出，因此读取 GeoJSON 文件后再原样直接写回时，会还原出该文档 (为该文件推断出的列是 `id`、`geometry` 和 `properties`) ：

```sql title="Query"
SELECT * FROM file('london.geojson', GeoJSON) FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"feature_type":"landmark","name":"Tower of London","year_built":1078}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"feature_type":"river","length_km":346,"name":"River Thames"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"area_km2":1.42,"feature_type":"park","name":"Hyde Park"}}]}
```

数值类型的 `id` 列会写为 JSON 数字 (若 `Nullable` `id` 为 `NULL`，则会被完全省略) ：

```sql title="Query"
SELECT 42 AS id, (-0.1276, 51.5072)::Point AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":42,"geometry":{"type":"Point","coordinates":[-0.1276,51.5072]},"properties":{}}]}
```

`Ring` 写作单环 `Polygon`：

```sql title="Query"
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,0]]]},"properties":{}}]}
```

<div id="writing-to-a-file">
  ### 写入文件
</div>

使用 `INTO OUTFILE` 将客户端中的数据写入 GeoJSON 文件：

```sql title="Query"
SELECT id, geometry, properties
FROM london
ORDER BY id
INTO OUTFILE 'london_export.geojson'
FORMAT GeoJSON;
```

服务器本身也可以使用 `file` 表函数写入该文件 (`.geojson` 扩展名会自动选择格式) ：

```sql title="Query"
INSERT INTO FUNCTION file('london_export.geojson', GeoJSON)
SELECT id, geometry, properties FROM london;
```

<div id="writing-limitations">
  ### 限制
</div>

:::note
ClickHouse 的 geo types 不携带坐标参考系统，因此输出会假定坐标已是 WGS84 的经纬度，并按 `[longitude, latitude]` 顺序排列，这也是 [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-4) 的要求。不会执行重投影或坐标轴交换，因此投影坐标——或以 `(latitude, longitude)` 形式存储的数据——会生成结构有效但不符合规范的 GeoJSON。
:::

输出只反映 ClickHouse 中实际存储的内容：

* 读取时被丢弃的信息——位置高程、`bbox`、外来成员，以及 `id` 是字符串还是数值的区别——都无法还原；请参见[读取限制](#reading-limitations)。
* 坐标会使用 `Float64` 值可往返的最短表示形式写出。
* 直接取自 `JSON` 列的 `properties` 对象会按 `JSON` 类型的 canonical 键顺序输出，这可能与输入不同。

几何对象会严格按存储时的样子写出——坐标顺序和 winding 都会保留。默认情况下，写出时会强制执行 GeoJSON 形态有效性校验 (参见[几何校验](#geometry-validation)) ：不属于有效 GeoJSON 形态的几何对象，例如只有一个点的 `LineString` 或未闭合的 `Polygon` 环，会被拒绝，以确保写出的文档能够再读回来。将 `format_geojson_validate_geometry = 0` 设为 0，则会原样输出这类几何对象，从而生成结构有效但不符合规范的 GeoJSON。无论采用哪种方式，都不会强制执行右手法则 (winding) 不变性，同时也会保留 `null` 与空 `properties` 对象之间的区别。

<div id="geometry-validation">
  ## 几何校验
</div>

设置 `format_geojson_validate_geometry` 用于控制该格式在读写两个方向上是否强制执行 [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1) 的几何形状规则。该设置默认启用。

启用后，违反 GeoJSON 形状规则的几何对象会被拒绝：点数少于两个的 `LineString` (或 `MultiLineString` 中的一条线) ；点数少于四个的 `Polygon` 或 `MultiPolygon` 的 Ring，或首尾点不一致 (即未闭合的 Ring) ；以及空的 `MultiLineString`、`Polygon` 或 `MultiPolygon`。读取这类文档和写出这类 ClickHouse 值时都适用相同规则，因此写出的文档始终可以再读回。

禁用后，这些形状规则在两个方向上都不会强制执行：退化的几何对象会按原样读取，也会按原样写出。这样一来，那些并非有效 GeoJSON 几何对象的 ClickHouse 几何值也可以通过该格式往返读写，但代价是会生成不符合 GeoJSON 规范的文档。

这种校验仅限于结构层面：只检查点数以及 Ring 是否闭合。它不会检查形状在几何意义上的正确性，因此，结构上有效但几何上退化的几何对象在两个方向上都会被接受——例如面积为零的多边形、自相交的 Ring，或孔 (内环) 位于外环之外的多边形。同样，Polygon 的 Ring 是否满足右手法则 (winding) 方向也永远不会被强制要求。

有一项检查不受该设置影响：非有限值坐标 (`NaN`、`Inf`) 始终会被拒绝，因为它们无法表示为 JSON 数字。