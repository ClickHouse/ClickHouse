---
toc_priority: 62
toc_title: Geographical Coordinates
---

# Functions for Working with Geographical Coordinates {#functions-for-working-with-geographical-coordinates}

## greatCircleDistance {#greatcircledistance}

Calculates the distance between two points on the Earth’s surface using [the great-circle formula](https://en.wikipedia.org/wiki/Great-circle_distance).

``` sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Input parameters**

-   `lon1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]`.
-   `lat1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]`.
-   `lon2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]`.
-   `lat2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]`.

Positive values correspond to North latitude and East longitude, and negative values correspond to South latitude and West longitude.

**Returned value**

The distance between two points on the Earth’s surface, in meters.

Generates an exception when the input parameter values fall outside of the range.

**Example**

``` sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

``` text
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## greatCircleAngle {#greatcircleangle}

Calculates the central angle between two points on the Earth’s surface using [the great-circle formula](https://en.wikipedia.org/wiki/Great-circle_distance).

``` sql
greatCircleAngle(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Input parameters**

-   `lon1Deg` — Longitude of the first point in degrees.
-   `lat1Deg` — Latitude of the first point in degrees.
-   `lon2Deg` — Longitude of the second point in degrees.
-   `lat2Deg` — Latitude of the second point in degrees.

**Returned value**

The central angle between two points in degrees.

**Example**

``` sql
SELECT greatCircleAngle(0, 0, 45, 0) AS arc
```

``` text
┌─arc─┐
│  45 │
└─────┘
```

## pointInEllipses {#pointinellipses}

Checks whether the point belongs to at least one of the ellipses.
Coordinates are geometric in the Cartesian coordinate system.

``` sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Input parameters**

-   `x, y` — Coordinates of a point on the plane.
-   `xᵢ, yᵢ` — Coordinates of the center of the `i`-th ellipsis.
-   `aᵢ, bᵢ` — Axes of the `i`-th ellipsis in units of x, y coordinates.

The input parameters must be `2+4⋅n`, where `n` is the number of ellipses.

**Returned values**

`1` if the point is inside at least one of the ellipses; `0`if it is not.

**Example**

``` sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

``` text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## pointInPolygon {#pointinpolygon}

Checks whether the point belongs to the polygon on the plane.

``` sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Input values**

-   `(x, y)` — Coordinates of a point on the plane. Data type — [Tuple](../../sql-reference/data-types/tuple.md) — A tuple of two numbers.
-   `[(a, b), (c, d) ...]` — Polygon vertices. Data type — [Array](../../sql-reference/data-types/array.md). Each vertex is represented by a pair of coordinates `(a, b)`. Vertices should be specified in a clockwise or counterclockwise order. The minimum number of vertices is 3. The polygon must be constant.
-   The function also supports polygons with holes (cut out sections). In this case, add polygons that define the cut out sections using additional arguments of the function. The function does not support non-simply-connected polygons.

**Returned values**

`1` if the point is inside the polygon, `0` if it is not.
If the point is on the polygon boundary, the function may return either 0 or 1.

**Example**

``` sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## geohashEncode {#geohashencode}

Encodes latitude and longitude as a geohash-string, please see (http://geohash.org/, https://en.wikipedia.org/wiki/Geohash).

``` sql
geohashEncode(longitude, latitude, [precision])
```

**Input values**

-   longitude - longitude part of the coordinate you want to encode. Floating in range`[-180°, 180°]`
-   latitude - latitude part of the coordinate you want to encode. Floating in range `[-90°, 90°]`
-   precision - Optional, length of the resulting encoded string, defaults to `12`. Integer in range `[1, 12]`. Any value less than `1` or greater than `12` is silently converted to `12`.

**Returned values**

-   alphanumeric `String` of encoded coordinate (modified version of the base32-encoding alphabet is used).

**Example**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res
```

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode {#geohashdecode}

Decodes any geohash-encoded string into longitude and latitude.

**Input values**

-   encoded string - geohash-encoded string.

**Returned values**

-   (longitude, latitude) - 2-tuple of `Float64` values of longitude and latitude.

**Example**

``` sql
SELECT geohashDecode('ezs42') AS res
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geoToH3 {#geotoh3}

Returns [H3](https://uber.github.io/h3/#/documentation/overview/introduction) point index `(lon, lat)` with specified resolution.

[H3](https://uber.github.io/h3/#/documentation/overview/introduction) is a geographical indexing system where Earth’s surface divided into even hexagonal tiles. This system is hierarchical, i. e. each hexagon on the top level can be splitted into seven even but smaller ones and so on.

This index is used primarily for bucketing locations and other geospatial manipulations.

**Syntax**

``` sql
geoToH3(lon, lat, resolution)
```

**Parameters**

-   `lon` — Longitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `lat` — Latitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned values**

-   Hexagon index number.
-   0 in case of error.

Type: `UInt64`.

**Example**

Query:

``` sql
SELECT geoToH3(37.79506683, 55.71290588, 15) as h3Index
```

Result:

``` text
┌────────────h3Index─┐
│ 644325524701193974 │
└────────────────────┘
```

## geohashesInBox {#geohashesinbox}

Returns an array of geohash-encoded strings of given precision that fall inside and intersect boundaries of given box, basically a 2D grid flattened into array.

**Input values**

-   longitude\_min - min longitude, floating value in range `[-180°, 180°]`
-   latitude\_min - min latitude, floating value in range `[-90°, 90°]`
-   longitude\_max - max longitude, floating value in range `[-180°, 180°]`
-   latitude\_max - max latitude, floating value in range `[-90°, 90°]`
-   precision - geohash precision, `UInt8` in range `[1, 12]`

Please note that all coordinate parameters should be of the same type: either `Float32` or `Float64`.

**Returned values**

-   array of precision-long strings of geohash-boxes covering provided area, you should not rely on order of items.
-   \[\] - empty array if *min* values of *latitude* and *longitude* aren’t less than corresponding *max* values.

Please note that function will throw an exception if resulting array is over 10’000’000 items long.

**Example**

``` sql
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos
```

``` text
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```

## h3GetBaseCell {#h3getbasecell}

Returns the base cell number of the H3 index.

**Syntax**

``` sql
h3GetBaseCell(index)
```

**Parameter**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hexagon base cell number. 

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT h3GetBaseCell(612916788725809151) as basecell;
```

Result:

``` text
┌─basecell─┐
│       12 │
└──────────┘
```

## h3HexAreaM2 {#h3hexaream2}

Returns average hexagon area in square meters at the given resolution.

**Syntax**

``` sql
h3HexAreaM2(resolution)
```

**Parameter**

-   `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Area in square meters. 

Type: [Float64](../../sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT h3HexAreaM2(13) as area;
```

Result:

``` text
┌─area─┐
│ 43.9 │
└──────┘
```

## h3IndexesAreNeighbors {#h3indexesareneighbors}

Returns whether or not the provided H3 indexes are neighbors.

**Syntax**

``` sql
h3IndexesAreNeighbors(index1, index2)
```

**Parameters**

-   `index1` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `index2` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   `1` — Indexes are neighbours.
-   `0` — Indexes are not neighbours.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT h3IndexesAreNeighbors(617420388351344639, 617420388352655359) AS n;
```

Result:

``` text
┌─n─┐
│ 1 │
└───┘
```

## h3ToChildren {#h3tochildren}

Returns an array of child indexes for the given H3 index.

**Syntax**

``` sql
h3ToChildren(index, resolution)
```

**Parameters**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned values**

-   Array of the child H3-indexes. 

Type: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT h3ToChildren(599405990164561919, 6) AS children;
```

Result:

``` text
┌─children───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [603909588852408319,603909588986626047,603909589120843775,603909589255061503,603909589389279231,603909589523496959,603909589657714687] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## h3ToParent {#h3toparent}

Returns the parent (coarser) index containing the given H3 index.

**Syntax**

``` sql
h3ToParent(index, resolution)
```

**Parameters**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Parent H3 index. 

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT h3ToParent(599405990164561919, 3) as parent;
```

Result:

``` text
┌─────────────parent─┐
│ 590398848891879423 │
└────────────────────┘
```

## h3ToString {#h3tostring}

Converts the `H3Index` representation of the index to the string representation.

``` sql
h3ToString(index)
```

**Parameter**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   String representation of the H3 index. 

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT h3ToString(617420388352917503) as h3_string;
```

Result:

``` text
┌─h3_string───────┐
│ 89184926cdbffff │
└─────────────────┘
```

## stringToH3 {#stringtoh3}

Converts the string representation to the `H3Index` (UInt64) representation.

**Syntax**

``` sql
stringToH3(index_str)
```

**Parameter**

-   `index_str` — String representation of the H3 index. Type: [String](../../sql-reference/data-types/string.md).

**Returned value**

-   Hexagon index number. Returns 0 on error. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT stringToH3('89184926cc3ffff') as index;
```

Result:

``` text
┌──────────────index─┐
│ 617420388351344639 │
└────────────────────┘
```

## h3GetResolution {#h3getresolution}

Returns the resolution of the H3 index.

**Syntax**

``` sql
h3GetResolution(index)
```

**Parameter**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT h3GetResolution(617420388352917503) as res;
```

Result:

``` text
┌─res─┐
│   9 │
└─────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/functions/geo/) <!--hide-->
