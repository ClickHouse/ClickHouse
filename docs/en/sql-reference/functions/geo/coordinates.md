---
description: 'Documentation for Coordinates'
sidebar_label: 'Geographical Coordinates'
slug: /sql-reference/functions/geo/coordinates
title: 'Functions for Working with Geographical Coordinates'
doc_type: 'reference'
---

## greatCircleDistance {#greatcircledistance}

Calculates the distance between two points on the Earth's surface using [the great-circle formula](https://en.wikipedia.org/wiki/Great-circle_distance).

```sql
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

```sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673) AS greatCircleDistance
```

```text
┌─greatCircleDistance─┐
│            14128352 │
└─────────────────────┘
```

## geoDistance {#geodistance}

Similar to `greatCircleDistance` but calculates the distance on WGS-84 ellipsoid instead of sphere. This is more precise approximation of the Earth Geoid.
The performance is the same as for `greatCircleDistance` (no performance drawback). It is recommended to use `geoDistance` to calculate the distances on Earth.

Technical note: for close enough points we calculate the distance using planar approximation with the metric on the tangent plane at the midpoint of the coordinates.

```sql
geoDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
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

```sql
SELECT geoDistance(38.8976, -77.0366, 39.9496, -75.1503) AS geoDistance
```

```text
┌─geoDistance─┐
│   212458.73 │
└─────────────┘
```

## greatCircleAngle {#greatcircleangle}

Calculates the central angle between two points on the Earth's surface using [the great-circle formula](https://en.wikipedia.org/wiki/Great-circle_distance).

```sql
greatCircleAngle(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Input parameters**

- `lon1Deg` — Longitude of the first point in degrees.
- `lat1Deg` — Latitude of the first point in degrees.
- `lon2Deg` — Longitude of the second point in degrees.
- `lat2Deg` — Latitude of the second point in degrees.

**Returned value**

The central angle between two points in degrees.

**Example**

```sql
SELECT greatCircleAngle(0, 0, 45, 0) AS arc
```

```text
┌─arc─┐
│  45 │
└─────┘
```

## geoToUTM {#geotoutm}

Converts WGS84 geographic coordinates `(longitude, latitude)` to [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) coordinates.

UTM is a set of 60 transverse Mercator projections, each covering a 6°-wide longitudinal zone, that map geographic coordinates to a planar grid in metres. The zone is selected automatically from the longitude, applying the standard exceptions for Norway and Svalbard, unless an explicit `zone` is given. UTM is only defined for latitudes in the range `[-80°, 84°]`; the polar caps use the separate UPS system.

```sql
geoToUTM(longitude, latitude[, zone])
```

**Arguments**

- `longitude` — Longitude in degrees. Range: `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
- `latitude` — Latitude in degrees. Range: `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
- `zone` — Optional. Force the projection into this UTM zone instead of selecting it automatically. Range: `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).

**Returned value**

A named tuple `(easting, northing, zone, band)`: `easting` and `northing` in metres ([`Float64`](../../data-types/float.md)), the UTM `zone` number ([`UInt8`](../../data-types/int-uint.md)), and the MGRS latitude `band` letter ([`FixedString(1)`](../../data-types/fixedstring.md)). A `band` of `'N'` or later denotes the northern hemisphere.

Generates an exception when the latitude is outside `[-80°, 84°]` or the longitude is outside `[-180°, 180°]`.

**Example**

```sql
SELECT geoToUTM(2.294497, 48.858222) AS utm; -- Eiffel Tower
```

```text
(448251.5978370684,5411935.125629659,31,'U')
```

## UTMToGeo {#utmtogeo}

Converts [UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) coordinates back to WGS84 geographic coordinates `(longitude, latitude)`. This is the inverse of [`geoToUTM`](#geotoutm).

```sql
UTMToGeo(easting, northing, zone, is_north)
```

**Arguments**

- `easting` — Easting in metres (includes the 500000 m false easting). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
- `northing` — Northing in metres (includes the 10000000 m false northing on the southern hemisphere). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
- `zone` — UTM zone number. Range: `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).
- `is_north` — Hemisphere: `1` for the northern hemisphere, `0` for the southern. [`(U)Int*`](../../data-types/int-uint.md).

**Returned value**

A named tuple `(longitude, latitude)` in degrees. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Example**

```sql
SELECT UTMToGeo(448251.6, 5411935.13, 31, 1) AS coord;
```

```text
(2.2944970289079203,48.85822204127082)
```

## geoToMGRS {#geotomgrs}

Encodes WGS84 geographic coordinates `(longitude, latitude)` as a [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) string.

The string is `<zone><band><100km square><easting><northing>`, for example `31UDQ4825111935`. The `precision` argument controls the number of digits used for each of the easting and northing: `5` (default) for 1 m, `4` for 10 m, `3` for 100 m, `2` for 1 km, `1` for 10 km, and `0` for the 100 km grid square only. MGRS is only defined for latitudes in the range `[-80°, 84°]`.

```sql
geoToMGRS(longitude, latitude[, precision])
```

**Arguments**

- `longitude` — Longitude in degrees. Range: `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
- `latitude` — Latitude in degrees. Range: `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
- `precision` — Optional. Number of digits for each of easting and northing. Default: `5`. Range: `[0, 5]`. [`(U)Int*`](../../data-types/int-uint.md).

**Returned value**

The MGRS reference string. [`String`](../../data-types/string.md).

**Example**

```sql
SELECT geoToMGRS(2.294497, 48.858222) AS mgrs, geoToMGRS(2.294497, 48.858222, 3) AS mgrs_100m;
```

```text
┌─mgrs────────────┬─mgrs_100m───┐
│ 31UDQ4825111935 │ 31UDQ482119 │
└─────────────────┴─────────────┘
```

## MGRSToGeo {#mgrstogeo}

Decodes an [MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) string into WGS84 geographic coordinates `(longitude, latitude)`. This is the inverse of [`geoToMGRS`](#geotomgrs).

The returned point is the centre of the referenced grid square, so the precision of the result matches the precision encoded in the string. Whitespace in the input is ignored and letters are case-insensitive.

```sql
MGRSToGeo(mgrs)
```

**Arguments**

- `mgrs` — MGRS reference string to decode. [`String`](../../data-types/string.md)/[`FixedString`](../../data-types/fixedstring.md).

**Returned value**

A named tuple `(longitude, latitude)` in degrees. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Example**

```sql
SELECT MGRSToGeo('31UDQ4825111935') AS coord;
```

```text
(2.294495618908297,48.85822536113692)
```

## pointInEllipses {#pointinellipses}

Checks whether the point belongs to at least one of the ellipses.
Coordinates are geometric in the Cartesian coordinate system.

```sql
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

```sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

```text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## pointInPolygon {#pointinpolygon}

Checks whether the point belongs to the polygon on the plane.

```sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Input values**

- `(x, y)` — Coordinates of a point on the plane. Data type — [Tuple](../../data-types/tuple.md) — A tuple of two numbers, or a [Point](../../data-types/geo.md/#point).
- `[(a, b), (c, d) ...]` — Polygon vertices. Data type — [Array](../../data-types/array.md) or [Ring](../../data-types/geo.md/#ring). Each vertex is represented by a pair of coordinates `(a, b)`. Vertices should be specified in a clockwise or counterclockwise order. The minimum number of vertices is 3.
- The function supports polygon with holes (cut-out sections). Data type — [Polygon](../../data-types/geo.md/#polygon). Either pass the entire `Polygon` as the second argument, or pass the outer ring first and then each hole as separate additional arguments.
- The function also supports multipolygon. Data type — [MultiPolygon](../../data-types/geo.md/#multipolygon). Either pass the entire `MultiPolygon` as the second argument, or list each component polygon as its own argument.
- The polygon argument can also be a [Geometry](../../data-types/geo.md/#geometry) column holding polygon-shaped values (`Ring`, `Polygon`, or `MultiPolygon`).

The polygon-shaped types ([Ring](../../data-types/geo.md/#ring), [Polygon](../../data-types/geo.md/#polygon), [MultiPolygon](../../data-types/geo.md/#multipolygon) and [Geometry](../../data-types/geo.md/#geometry)) may be passed either as constants or as regular (non-constant) table columns. When the polygon is provided across several separate arguments (an outer ring followed by holes, or several polygons of a multipolygon), all of those arguments must be constant.

**Returned values**

`1` if the point is inside the polygon, `0` if it is not.
If the point is on the polygon boundary, the function may return either 0 or 1.

**Example**

```sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```text
┌─res─┐
│   1 │
└─────┘
```

The polygon can also be given using the named geometric data types, including as a table column:

```sql
CREATE TABLE poly (id UInt32, shape Polygon) ENGINE = Memory;
INSERT INTO poly VALUES (1, [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
SELECT id, pointInPolygon((2., 2.), shape) AS res FROM poly;
```

```text
┌─id─┬─res─┐
│  1 │   1 │
└────┴─────┘
```

> **Note**  
> • You can set `validate_polygons = 0` to bypass geometry validation.  
> • `pointInPolygon` assumes every polygon is well-formed. If the input is self-intersecting, has mis-ordered rings, or overlapping edges, results become unreliable—especially for points that sit exactly on an edge, a vertex, or inside a self-intersection where the notion of "inside" vs. "outside" is undefined.
> • When the polygon argument is constant and the point is expressed using indexed key columns (for example, `pointInPolygon((x, y), constant_polygon)` on a table where `x, y` are part of the `PRIMARY KEY` or covered by a `minmax` index), ClickHouse can use both the primary key and `minmax` data-skipping indexes to prune irrelevant granules.
