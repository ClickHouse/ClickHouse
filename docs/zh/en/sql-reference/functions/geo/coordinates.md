---
description: '地理坐标相关文档'
sidebar_label: '地理坐标'
slug: /sql-reference/functions/geo/coordinates
title: '地理坐标处理函数'
doc_type: 'reference'
---

<div id="greatcircledistance">
  ## greatCircleDistance
</div>

使用[大圆距离公式](https://en.wikipedia.org/wiki/Great-circle_distance)计算地球表面上两点之间的距离。

```sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**输入参数**

* `lon1Deg` — 第一个点的经度 (以度为单位) 。范围：`[-180°, 180°]`。
* `lat1Deg` — 第一个点的纬度 (以度为单位) 。范围：`[-90°, 90°]`。
* `lon2Deg` — 第二个点的经度 (以度为单位) 。范围：`[-180°, 180°]`。
* `lat2Deg` — 第二个点的纬度 (以度为单位) 。范围：`[-90°, 90°]`。

正值表示北纬和东经，负值表示南纬和西经。

**返回值**

地球表面两点之间的距离，单位为米。

当输入参数的值超出范围时，会抛出异常。

**示例**

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

与 `greatCircleDistance` 类似，但它计算的是 WGS-84 椭球体上的距离，而不是球面上的距离。这种方式对地球大地水准面的近似更精确。
其性能与 `greatCircleDistance` 相同 (没有性能损耗) 。建议使用 `geoDistance` 来计算地球上的距离。

技术说明：对于距离足够近的点，我们使用平面近似来计算距离，并采用坐标中点处切平面上的度量。

```sql
geoDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**输入参数**

* `lon1Deg` — 第一个点的经度 (以度为单位) 。范围：`[-180°, 180°]`。
* `lat1Deg` — 第一个点的纬度 (以度为单位) 。范围：`[-90°, 90°]`。
* `lon2Deg` — 第二个点的经度 (以度为单位) 。范围：`[-180°, 180°]`。
* `lat2Deg` — 第二个点的纬度 (以度为单位) 。范围：`[-90°, 90°]`。

正值对应北纬和东经，负值对应南纬和西经。

**返回值**

地球表面两点之间的距离，单位为米。

当输入参数值超出范围时，会抛出异常。

**示例**

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

使用[大圆公式](https://en.wikipedia.org/wiki/Great-circle_distance)计算地球表面两点之间的中心角。

```sql
greatCircleAngle(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**输入参数**

* `lon1Deg` — 第一个点的经度，以度为单位。
* `lat1Deg` — 第一个点的纬度，以度为单位。
* `lon2Deg` — 第二个点的经度，以度为单位。
* `lat2Deg` — 第二个点的纬度，以度为单位。

**返回值**

两个点之间的中心角，以度为单位。

**示例**

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

将 WGS84 地理坐标 `(longitude, latitude)` 转换为 [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) 坐标。

UTM 是一组由 60 个横轴墨卡托投影构成的坐标系统，每个投影覆盖一个宽 6° 的经度带，用于将地理坐标映射到以米为单位的平面网格。除非显式指定 `zone`，否则会根据经度自动选择分区，并应用适用于挪威和斯瓦尔巴群岛的标准例外规则。UTM 仅定义于 `[-80°, 84°]` 范围内的纬度；两极地区则使用独立的 UPS 系统。

```sql
geoToUTM(longitude, latitude[, zone])
```

**参数**

* `longitude` — 以度为单位的经度。范围：`[-180°, 180°]`。[`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md)。
* `latitude` — 以度为单位的纬度。范围：`[-80°, 84°]`。[`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md)。
* `zone` — 可选。强制投影到该 UTM 分区，而不是自动选择分区。范围：`[1, 60]`。[`(U)Int*`](../../data-types/int-uint.md)。

**返回值**

一个命名元组 `(easting, northing, zone, band)`：`easting` 和 `northing` 的单位为米 ([`Float64`](../../data-types/float.md)) ，UTM `zone` 编号 ([`UInt8`](../../data-types/int-uint.md)) ，以及 MGRS 纬度 `band` 字母 ([`FixedString(1)`](../../data-types/fixedstring.md)) 。`band` 为 `'N'` 或其后的字母表示北半球。

当纬度超出 `[-80°, 84°]` 或经度超出 `[-180°, 180°]` 时，会引发异常。

**示例**

```sql
SELECT geoToUTM(2.294497, 48.858222) AS utm; -- Eiffel Tower
```

```text
(448251.5978370684,5411935.125629659,31,'U')
```

<div id="utmtogeo">
  ## UTMToGeo
</div>

将 [UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) 坐标转换回 WGS84 地理坐标 `(longitude, latitude)`。这是 [`geoToUTM`](#geotoutm) 的逆运算。

```sql
UTMToGeo(easting, northing, zone, is_north)
```

**参数**

* `easting` — 以米为单位的东坐标 (包含 500000 m 的假东移) 。[`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md)。
* `northing` — 以米为单位的北坐标 (包含南半球的 10000000 m 假北移) 。[`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md)。
* `zone` — UTM 分区号。范围：`[1, 60]`。[`(U)Int*`](../../data-types/int-uint.md)。
* `is_north` — 半球：北半球为 `1`，南半球为 `0`。[`(U)Int*`](../../data-types/int-uint.md)。

**返回值**

以度为单位的命名元组 `(longitude, latitude)`。[`Tuple(Float64, Float64)`](../../data-types/tuple.md)。

**示例**

```sql
SELECT UTMToGeo(448251.6, 5411935.13, 31, 1) AS coord;
```

```text
(2.2944970289079203,48.85822204127082)
```

<div id="geotomgrs">
  ## geoToMGRS
</div>

将 WGS84 地理坐标 `(longitude, latitude)` 编码为 [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) 字符串。

该字符串的格式为 `<zone><band><100km square><easting><northing>`，例如 `31UDQ4825111935`。`precision` 参数控制东向坐标和北向坐标各自使用的位数：`5` (默认) 表示 1 m，`4` 表示 10 m，`3` 表示 100 m，`2` 表示 1 km，`1` 表示 10 km，`0` 则仅表示 100 km 网格方块。MGRS 仅适用于 `[-80°, 84°]` 范围内的纬度。

```sql
geoToMGRS(longitude, latitude[, precision])
```

**参数**

* `longitude` — 以度为单位的经度。范围：`[-180°, 180°]`。[`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md)。
* `latitude` — 以度为单位的纬度。范围：`[-80°, 84°]`。[`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md)。
* `precision` — 可选。东向坐标和北向坐标各自的位数。默认值：`5`。范围：`[0, 5]`。[`(U)Int*`](../../data-types/int-uint.md)。

**返回值**

MGRS 字符串表示。[`String`](../../data-types/string.md)。

**示例**

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

将 [MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) 字符串解码为 WGS84 地理坐标 `(longitude, latitude)`。这是 [`geoToMGRS`](#geotomgrs) 的逆过程。

返回的 Point 是所指网格方块的中心，因此结果的 precision 与字符串中编码的 precision 一致。输入中的空白字符会被忽略，字母不区分大小写。

```sql
MGRSToGeo(mgrs)
```

**参数**

* `mgrs` — 要解码的 MGRS 参考字符串。[`String`](../../data-types/string.md)/[`FixedString`](../../data-types/fixedstring.md)。

**返回值**

以度为单位的命名元组 `(longitude, latitude)`。[`Tuple(Float64, Float64)`](../../data-types/tuple.md)。

**示例**

```sql
SELECT MGRSToGeo('31UDQ4825111935') AS coord;
```

```text
(2.294495618908297,48.85822536113692)
```

<div id="pointinellipses">
  ## pointInEllipses
</div>

检查该点是否位于至少一个椭圆内。
坐标为笛卡尔坐标系中的几何坐标。

```sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**输入参数**

* `x, y` — 平面上一点的坐标。
* `xᵢ, yᵢ` — 第 `i` 个椭圆的中心坐标。
* `aᵢ, bᵢ` — 第 `i` 个椭圆在 x、y 坐标单位下的轴长。

输入参数总数必须为 `2+4⋅n`，其中 `n` 为椭圆的数量。

**返回值**

如果该点位于至少一个椭圆内，则返回 `1`；否则返回 `0`。

**示例**

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

检查该点是否位于平面上的多边形内。

```sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**输入值**

* `(x, y)` — 平面上一点的坐标。数据类型 — [Tuple](../../data-types/tuple.md) — 由两个数字组成的元组。
* `[(a, b), (c, d) ...]` — Polygon 的顶点。数据类型 — [Array](../../data-types/array.md)。每个顶点由一对坐标 `(a, b)` 表示。顶点应按顺时针或逆时针顺序指定。顶点的最少数量为 3。该 Polygon 必须是常量。
* 该函数支持带孔的 Polygon (挖空区域) 。数据类型 — [Polygon](../../data-types/geo.md/#polygon)。可以将整个 `Polygon` 作为第二个参数传入，或者先传入外环，再将每个孔作为单独的附加参数传入。
* 该函数也支持 multipolygon。数据类型 — [MultiPolygon](../../data-types/geo.md/#multipolygon)。可以将整个 `MultiPolygon` 作为第二个参数传入，或者将每个组成 Polygon 分别作为单独的参数传入。

**返回值**

如果点位于 Polygon 内部，则返回 `1`；否则返回 `0`。
如果点位于 Polygon 边界上，函数可能返回 `0` 或 `1`。

**示例**

```sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```text
┌─res─┐
│   1 │
└─────┘
```

> **注意**
> • 你可以设置 `validate_polygons = 0` 以绕过几何校验。
> • `pointInPolygon` 假定每个多边形都是合法构造的。如果输入存在自相交、环顺序错误或边重叠，结果就可能不可靠——尤其是对于恰好落在边上、顶点上，或位于自相交区域内的点，因为此时 &quot;内部&quot; 与 &quot;外部&quot; 的定义并不明确。
> • 当多边形参数是常量，且点由已建立索引的键列表示时 (例如，在 `x, y` 属于 `PRIMARY KEY` 或受 `minmax` 索引覆盖的表上执行 `pointInPolygon((x, y), constant_polygon)`) ，ClickHouse 可以同时使用主键和 `minmax` 数据跳过索引来剪枝无关粒度。