# GEO函数

## greatCircleDistance

使用[great-circle distance公式](https://en.wikipedia.org/wiki/Great-circle_distance)计算地球表面两点之间的距离。

```
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**输入参数**

- `lon1Deg` — 第一个点的经度，单位：度，范围： `[-180°, 180°]`。
- `lat1Deg` — 第一个点的纬度，单位：度，范围： `[-90°, 90°]`。
- `lon2Deg` — 第二个点的经度，单位：度，范围： `[-180°, 180°]`。
- `lat2Deg` — 第二个点的纬度，单位：度，范围： `[-90°, 90°]`。

正值对应北纬和东经，负值对应南纬和西经。

**返回值**

地球表面的两点之间的距离，以米为单位。

当输入参数值超出规定的范围时将抛出异常。

**示例**

``` sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

```
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## pointInEllipses

检查指定的点是否至少包含在一个指定的椭圆中。

```
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**输入参数**

- `x, y` — 平面上某个点的坐标。
- `xᵢ, yᵢ` — 第i个椭圆的中心坐标。
- `aᵢ, bᵢ` — 第i个椭圆的轴，单位：米。

输入参数的个数必须是`2+4⋅n`，其中`n`是椭圆的数量。

**返回值**

如果该点至少包含在一个椭圆中，则返回`1`；否则，则返回`0`。

**示例**

``` sql
SELECT pointInEllipses(55.755831, 37.617673, 55.755831, 37.617673, 1.0, 2.0)
```

```
┌─pointInEllipses(55.755831, 37.617673, 55.755831, 37.617673, 1., 2.)─┐
│                                                                   1 │
└─────────────────────────────────────────────────────────────────────┘
```

## pointInPolygon

检查指定的点是否包含在指定的多边形中。

```
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**输入参数**

- `(x, y)` — 平面上某个点的坐标。[Tuple](../../data_types/tuple.md)类型，包含坐标的两个数字。
- `[(a, b), (c, d) ...]` — 多边形的顶点。[Array](../../data_types/array.md)类型。每个顶点由一对坐标`(a, b)`表示。顶点可以按顺时针或逆时针指定。顶点的个数应该大于等于3。同时只能是常量的。
- 该函数还支持镂空的多边形（切除部分）。如果需要，可以使用函数的其他参数定义需要切除部分的多边形。(The function does not support non-simply-connected polygons.)

**返回值**

如果坐标点存在在多边形范围内，则返回`1`。否则返回`0`。
如果坐标位于多边形的边界上，则该函数可能返回`1`，或可能返回`0`。

**示例**

``` sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```
┌─res─┐
│   1 │
└─────┘
```

## geohashEncode

将经度和纬度编码为geohash-string，请参阅（http://geohash.org/,https://en.wikipedia.org/wiki/Geohash）。
```
geohashEncode(longitude, latitude, [precision])
```

**输入值**

- longitude - 要编码的坐标的经度部分。其值应在`[-180°，180°]`范围内
- latitude - 要编码的坐标的纬度部分。其值应在`[-90°，90°]`范围内
- precision - 可选，生成的geohash-string的长度，默认为`12`。取值范围为`[1,12]`。任何小于`1`或大于`12`的值都会默认转换为`12`。

**返回值**

- 坐标编码的字符串（使用base32编码的修改版本）。

**示例**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res
```

```
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode

将任何geohash编码的字符串解码为经度和纬度。

**输入值**

- encoded string - geohash编码的字符串。

**返回值**

- (longitude, latitude) - 经度和纬度的`Float64`值的2元组。

**示例**

``` sql
SELECT geohashDecode('ezs42') AS res
```

```
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

[来源文章](https://clickhouse.yandex/docs/en/query_language/functions/geo/) <!--hide-->
