---
description: 'Mapbox 矢量瓦片编码文档'
sidebar_label: 'Mapbox 矢量瓦片'
sidebar_position: 65
slug: /sql-reference/functions/geo/mvt
title: 'Mapbox 矢量瓦片编码函数'
doc_type: 'reference'
---

<div id="overview">
  ## 概述
</div>

[Mapbox 矢量瓦片](https://github.com/mapbox/vector-tile-spec) (MVT) 是以 Protobuf 编码的瓦片，MapLibre 和 Mapbox GL 等 Web 地图
客户端可原生渲染。ClickHouse 仅需使用 SQL，借助一对相互配合的
函数即可构建这类瓦片：

* `MVTEncodeGeom` — 一个标量函数，用于将几何对象投影到 slippy-map 瓦片的本地像素空间中，并
  将其裁剪到瓦片范围内。
* `MVTEncode` — 一个聚合函数，用于将一组投影后的几何对象收集为
  单图层瓦片的二进制字节。

两个辅助函数 `MVTBoundingBox` 和 `MVTBoundingBoxMercator` 会返回瓦片的边界框，以便使用索引在 `WHERE` 子句中将行
限制在该范围内。

支持点、线和Polygon 几何类型，包括 `Geometry` 类型以及具体的 Geo 类型 (`Point`、
`LineString`、`MultiLineString`、`Ring`、`Polygon`、`MultiPolygon`) 。

生成的字节是一个完整的瓦片，可通过 HTTP 接口使用 `FORMAT RawBLOB` 直接返回。

这些函数与 PostGIS 的工作流程相对应，并且也提供了对应的 PostGIS 名称作为别名：`ST_AsMVTGeom`
对应 `MVTEncodeGeom`，`ST_AsMVT` 对应 `MVTEncode`。

<div id="mvtencodegeom">
  ## MVTEncodeGeom
</div>

将以地理坐标 (经度/纬度) 表示的几何对象投影到由 `zoom`、`tile_x` 和 `tile_y` 标识的
slippy-map 瓦片本地像素空间中，将其对齐到整数像素网格，裁剪到该瓦片范围内，
并返回瓦片空间几何对象。

该投影采用覆盖完整 `UInt32` 坐标范围的 Web Mercator。返回坐标的原点位于
瓦片左上角，y 轴向下，这正是 Mapbox 矢量瓦片
Tile 格式使用的坐标约定，因此结果可直接传给 `MVTEncode`。坐标会被四舍五入到整像素，因此按
`MVTEncodeGeom` 分组时，落在同一网格上的几何对象会归并为一个簇。

启用 `clip` 时 (默认如此) ，几何对象会被裁剪到按 `buffer` 像素向外扩展后的瓦片范围内 (每个轴上的范围为
`[-buffer, extent + buffer]`) ；完全落在该范围外的几何对象会变为 `NULL`。这对应于
PostGIS 的 `ST_AsMVTGeom`。

在校验之前，Polygon 坐标会被限制在一个 `2^30` 的范围内——这恰好是
`zoom` 为 18 且 `extent` 为 4096 时整个世界的像素跨度——因此对于实际使用的瓦片，几何对象会经过校验但绝不会被裁剪，而这一边界仅会在
`zoom` 或 `extent` 取极端值时影响几何对象。

输出几何类型取决于输入：`Point` 返回 `Point`；`LineString` 或 `MultiLineString` 返回
`MultiLineString`；`Ring`、`Polygon` 或 `MultiPolygon` 返回 `MultiPolygon` (裁剪可能会将一个几何对象拆分为
多个部分) 。

**语法**

```sql
MVTEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]])
```

**参数**

* `geometry` — 以经纬度 (度) 表示的 Geometry。经度会被限制在 `[-180, 180]` 范围内，纬度会被限制在 Web Mercator 范围 `[-85.05112878, 85.05112878]` 内。[`Point`](../../data-types/geo.md) / [`LineString`](../../data-types/geo.md) / [`MultiLineString`](../../data-types/geo.md) / [`Ring`](../../data-types/geo.md) / [`Polygon`](../../data-types/geo.md) / [`MultiPolygon`](../../data-types/geo.md) / [`Geometry`](../../data-types/geo.md)。
* `zoom` — Slippy-map 的缩放级别，范围为 `[0, 32]`。[`UInt8`](../../data-types/int-uint.md)。
* `tile_x` — 瓦片列索引，范围为 `[0, 2^zoom - 1]`。[`UInt32`](../../data-types/int-uint.md)。
* `tile_y` — 瓦片行索引，范围为 `[0, 2^zoom - 1]`。[`UInt32`](../../data-types/int-uint.md)。
* `extent` — 可选的瓦片边长 (每边像素数) ，范围为 `[1, 2147483647]`。默认值为 `4096`，即 Mapbox 矢量瓦片 的默认值。[`UInt32`](../../data-types/int-uint.md)。
* `buffer` — 可选的裁剪缓冲区大小 (像素) ，范围为 `[0, 2147483647]`。默认值为 `1`。[`UInt32`](../../data-types/int-uint.md)。
* `clip` — 可选标志；当其为非零值时 (默认如此) ，几何对象会被裁剪到瓦片及其缓冲区范围内。[`UInt8`](../../data-types/int-uint.md)。

**返回值**

返回瓦片空间中的几何对象；如果被完全裁剪掉，则返回 `NULL`。[`Geometry`](../../data-types/geo.md)。

**示例**

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

将一组要素编码为二进制的 Mapbox 矢量瓦片 layer。这是标量函数 `MVTEncodeGeom` 的聚合版本。每个输入行都会成为一个要素；支持点、线 和 Polygon 几何类型。

`geometry` argument 是采用瓦片空间坐标的 `Geometry`，通常由 `MVTEncodeGeom` 生成。`geometry` 为 `NULL` 的行 (例如被 `MVTEncodeGeom` 裁剪掉的行) 会被跳过。可选的 `properties` argument 是一个命名元组，其元素名称会成为要素 属性 的键，其元素 types 则决定 vector tile 的值类型。

结果是单 layer 瓦片的原始字节。空分组会生成空瓦片。这相当于 PostGIS 的 `ST_AsMVT`。

**语法**

```sql
MVTEncode(layer_name[, extent[, feature_id_name[, stringify_unsupported]]])(geometry[, properties])
```

**参数**

* `layer_name` — 矢量瓦片图层的名称。[`String`](../../data-types/string.md)。
* `extent` — 瓦片每条边的像素范围，取值区间为 `[1, 2147483647]`。默认值为 `4096`。[`UInt32`](../../data-types/int-uint.md)。
* `feature_id_name` — 可选名称，指定 `properties` 元组中一个无符号整数字段，将其作为 MVT Feature 的 `id` (`UInt64`) 输出，而不是作为标签。带符号整数会被拒绝。如果 `id` 为 `NULL`，则该要素会省略 `id`。参数按位置传递，因此要使用它，必须提供 `extent`。[`String`](../../data-types/string.md)。
* `stringify_unsupported` — 可选标志 (`0`/`1`，默认值为 `0`) ；当值为 `1` 时，无法直接支持的属性类型 (例如大整数、`UUID`、`Decimal`) 会编码为其文本 `string_value`，而不是报错。[`UInt8`](../../data-types/int-uint.md)。

**参数**

* `geometry` — 瓦片空间中的几何对象，例如来自 `MVTEncodeGeom`。[`Geometry`](../../data-types/geo.md)。
* `properties` — 可选的要素属性命名元组。元素名称会成为属性键。[`Tuple`](../../data-types/tuple.md)。

**返回值**

返回单图层 Mapbox 矢量瓦片 的二进制内容。[`String`](../../data-types/string.md)。

<div id="property-types">
  ### 属性类型
</div>

每个属性元素都会编码为与其 ClickHouse 类型相对应的 Mapbox 矢量瓦片 `Value` 变体：

| ClickHouse 类型                                                  | 矢量瓦片值类型        |
| -------------------------------------------------------------- | -------------- |
| `String` / `FixedString`                                       | `string_value` |
| `Float32` / `BFloat16`                                         | `float_value`  |
| `Float64`                                                      | `double_value` |
| `Bool`                                                         | `bool_value`   |
| `Int8` / `Int16` / `Int32` / `Int64` / `Date32`                | `sint_value`   |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` / `Date` / `DateTime` | `uint_value`   |

这些类型可以包裹在 `Nullable` 和/或 `LowCardinality` 中。对于 `NULL` 值，会省略该要素的该属性，因为
矢量瓦片格式不支持 null。其他任何属性类型都会引发异常，除非设置了 `stringify_unsupported`，此时
它会被编码为其文本形式的 `string_value`。

相同的属性值会归入该 layer 的共享值池，因此某个值即使出现在许多要素中，
也只会存储一次。

<div id="naming-the-properties-tuple">
  ### 为 properties 元组命名
</div>

properties 元组的元素必须显式命名。`tuple(...)` 内部的列别名**不会**传递给元组的
元素名，因此请使用类型转换来为这些元素命名：

```sql
tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)
```

<div id="clustering">
  ### 聚类
</div>

聚类是在 SQL 中实现的，而不是由该函数处理。由于 `MVTEncodeGeom` 会将坐标舍入到整像素，因此按
像素几何体分组会合并重合的几何对象；先在子查询中对各组进行聚合，然后将每个簇对应的一行数据传给
`MVTEncode`：

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

对 `Geometry` 值进行分组需要设置 `allow_suspicious_types_in_group_by = 1`，因为默认情况下，基于 `Variant` 的
`Geometry` 类型不允许用于分组。省略内部的 `GROUP BY` (以及 `count()`) 即可为每个输入行输出一个要素，
而不是输出聚合后的要素。

<div id="mvtboundingbox">
  ## MVTBoundingBox
</div>

返回由 `zoom`、`tile_x` 和 `tile_y` 标识的 slippy-map 瓦片的地理边界框，以元组
`(min_lon, min_lat, max_lon, max_lat)` 表示，单位为度。

可在直接对 `longitude`/`latitude` 列进行过滤时，使用它将行限制在某个瓦片范围内——这样就能利用这些列上的主键或
索引——而不必为每一行重新计算 Web Mercator 投影。可选参数 `margin`
会按瓦片大小的相应比例向边界框的各个方向扩展；将其设为 `buffer / extent`，即可覆盖
`MVTEncodeGeom` 的裁剪缓冲区。

**语法**

```sql
MVTBoundingBox(zoom, tile_x, tile_y[, margin])
```

**参数**

* `zoom` — Slippy-map 的缩放级别，范围为 `[0, 32]`。[`UInt8`](../../data-types/int-uint.md)。
* `tile_x` — 瓦片列索引，范围为 `[0, 2^zoom - 1]`。[`UInt32`](../../data-types/int-uint.md)。
* `tile_y` — 瓦片行索引，范围为 `[0, 2^zoom - 1]`。[`UInt32`](../../data-types/int-uint.md)。
* `margin` — 可选的瓦片尺寸比例，用于将边界框向四周扩展。默认值为 `0`。[`Float64`](../../data-types/float.md)。

**返回值**

返回以度为单位的瓦片边界框，表示为元组 `(min_lon, min_lat, max_lon, max_lat)`。[`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md)。

**示例**

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

`MVTBoundingBox` 的 Web Mercator 对应函数。返回
`MVTEncodeGeom` 在内部使用的完整 `UInt32` Web Mercator 坐标空间中该瓦片的
包围盒，表示为元组
`(min_x, min_y, max_x, max_y)`。y 轴向下增大 (北方位于顶部) 。适用于将
Mercator 坐标列物化，并对这些列而不是 `longitude`/`latitude` 建立索引的表。

**语法**

```sql
MVTBoundingBoxMercator(zoom, tile_x, tile_y[, margin])
```

**参数**

与 [`MVTBoundingBox`](#mvtboundingbox) 相同。

**返回值**

以 Web Mercator 坐标中的元组 `(min_x, min_y, max_x, max_y)` 形式返回瓦片边界框。[`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md)。

**示例**

```sql
SELECT MVTBoundingBoxMercator(1, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────┐
│ (0,0,2147483648,2147483648)  │
└──────────────────────────────┘
```

<div id="restricting-rows-to-a-tile">
  ## 将行限制在瓦片范围内
</div>

一个瓦片只能包含属于它的几何对象。最佳做法是通过两个相互配合的步骤来实现：在 `WHERE` 子句中使用可利用索引的低成本边界框谓词 (性能) ，以及对 `MVTEncodeGeom` 进行裁剪 (正确性) 。
裁剪会丢弃瓦片范围外的几何对象，因此即使边界框谓词较为宽松，也不会让瓦片外的几何对象泄漏到结果中。

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

边界框谓词只是一个粗略的预过滤器；精确的瓦片边界则由
`MVTEncodeGeom` 的裁剪操作来保证。向 `MVTEncodeGeom` 传递 `clip => false` (第七个参数) 即可禁用裁剪，并且仅依赖
`WHERE` 谓词。

<div id="serving-tiles-over-http">
  ## 通过 HTTP 提供瓦片
</div>

默认情况下，ClickHouse 不提供瓦片端点：HTTP 接口只在 `/` 上接受查询。Operator 通过
server configuration 中的[预定义查询处理程序](/zh/interfaces/http)添加了一个简洁的
`/tile/{z}/{x}/{y}` URL。该处理程序的 `url` 使用 `regex:` 形式捕获路径分段，并将其绑定到查询
参数，然后通过 `FORMAT RawBLOB` 返回字节数据。

在最简单的情况下，表中有一个 `Geometry` 列，处理程序会为每一行提供一个要素——`MVTEncodeGeom`
会将每个几何对象投影到请求的瓦片中并进行裁剪，因此位于瓦片之外的行会自动被过滤掉：

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

这里的 `shapes` 是一张包含 `geom Geometry` 列的表 (可混合包含点、线和多边形) 。`GET /tile/10/550/335`
会返回编码后的瓦片。

对于点数据，这种方式同样适用于普通的 `longitude`/`latitude` 列，只需通过
`MVTEncodeGeom((lon, lat)::Point, …)` 内联构建 Point 即可。要对重合的要素进行聚类，或为大型表添加利用索引的边界框预过滤器，
请按 [聚类](#clustering) 和
[将行限制在瓦片范围内](#restricting-rows-to-a-tile) 中所示扩展内层查询。

<div id="limitations">
  ## 局限性
</div>

* Web Mercator 投影会将纬度限制在 `±85.05112878°`，且不支持跨越反子午线的输入。