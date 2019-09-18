# Functions for working with geographical coordinates

## greatCircleDistance

Calculate the distance between two points on the Earth's surface using [the great-circle formula](https://en.wikipedia.org/wiki/Great-circle_distance).

```
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Input parameters**

- `lon1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]`.
- `lat1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]`.
- `lon2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]`.
- `lat2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]`.

Positive values correspond to North latitude and East longitude, and negative values correspond to South latitude and West longitude.

**Returned value**

The distance between two points on the Earth's surface, in meters.

Generates an exception when the input parameter values fall outside of the range.

**Example**

``` sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

```
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## pointInEllipses

Checks whether the point belongs to at least one of the ellipses.
Coordinates are geometric in the Cartesian coordinate system.

```
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Input parameters**

- `x, y` — Coordinates of a point on the plane.
- `xᵢ, yᵢ` — Coordinates of the center of the `i`-th ellipsis.
- `aᵢ, bᵢ` — Axes of the `i`-th ellipsis in units of x, y coordinates.

The input parameters must be `2+4⋅n`, where `n` is the number of ellipses.

**Returned values**

`1` if the point is inside at least one of the ellipses; `0`if it is not.

**Example**

``` sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

```
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## pointInPolygon

Checks whether the point belongs to the polygon on the plane.

```
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Input values**

- `(x, y)` — Coordinates of a point on the plane. Data type — [Tuple](../../data_types/tuple.md) —  A tuple of two numbers.
- `[(a, b), (c, d) ...]` — Polygon vertices. Data type — [Array](../../data_types/array.md). Each vertex is represented by a pair of coordinates `(a, b)`. Vertices should be specified in a clockwise or counterclockwise order. The minimum number of vertices is 3. The polygon must be constant.
- The function also supports polygons with holes (cut out sections). In this case, add polygons that define the cut out sections using additional arguments of the function. The function does not support non-simply-connected polygons.

**Returned values**

`1` if the point is inside the polygon, `0` if it is not.
If the point is on the polygon boundary, the function may return either 0 or 1.

**Example**

``` sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```
┌─res─┐
│   1 │
└─────┘
```

## geohashEncode

Encodes latitude and longitude as a geohash-string, please see (http://geohash.org/, https://en.wikipedia.org/wiki/Geohash).
```
geohashEncode(longitude, latitude, [precision])
```

**Input values**

- longitude - longitude part of the coordinate you want to encode. Floating in range`[-180°, 180°]`
- latitude - latitude part of the coordinate you want to encode. Floating in range `[-90°, 90°]`
- precision - Optional, length of the resulting encoded string, defaults to `12`. Integer in range `[1, 12]`. Any value less than `1` or greater than `12` is silently converted to `12`.

**Returned values**

- alphanumeric `String` of encoded coordinate (modified version of the base32-encoding alphabet is used).

**Example**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res
```

```
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode

Decodes any geohash-encoded string into longitude and latitude.

**Input values**

- encoded string - geohash-encoded string.

**Returned values**

- (longitude, latitude) - 2-tuple of `Float64` values of longitude and latitude.

**Example**

``` sql
SELECT geohashDecode('ezs42') AS res
```

```
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geoToH3

Calculates [H3](https://uber.github.io/h3/#/documentation/overview/introduction) point index `(lon, lat)` with specified resolution.

```
geoToH3(lon, lat, resolution)
```

**Input values**

- `lon` — Longitude. Type: [Float64](../../data_types/float.md).
- `lat` — Latitude. Type: [Float64](../../data_types/float.md).
- `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../data_types/int_uint.md).

**Returned values**

- Hexagon index number.
- 0 in case of error.

Type: [UInt64](../../data_types/int_uint.md).

**Example**

``` sql
SELECT geoToH3(37.79506683, 55.71290588, 15) as h3Index
```
```
┌────────────h3Index─┐
│ 644325524701193974 │
└────────────────────┘
```

## geohashesInBox

Returns an array of geohash-encoded strings of given precision that fall inside and intersect boundaries of given box, basically a 2D grid flattened into array.

**Input values**

- longitude_min - min longitude, floating value in range `[-180°, 180°]`
- latitude_min - min latitude, floating value in range `[-90°, 90°]`
- longitude_max - max longitude, floating value in range `[-180°, 180°]`
- latitude_max - max latitude, floating value in range `[-90°, 90°]`
- precision - geohash precision, `UInt8` in range `[1, 12]`

Please note that all coordinate parameters should be of the same type: either `Float32` or `Float64`.

**Returned values**

- array of precision-long strings of geohash-boxes covering provided area, you should not rely on order of items.
- [] - empty array if *min* values of *latitude* and *longitude* aren't less than corresponding *max* values.

Please note that function will throw an exception if resulting array is over 10'000'000 items long.

**Example**

```
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos
```
```
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/geo/) <!--hide-->
