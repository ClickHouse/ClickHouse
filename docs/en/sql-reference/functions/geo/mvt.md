---
description: 'Documentation for encoding Mapbox Vector Tiles'
sidebar_label: 'Mapbox Vector Tiles'
sidebar_position: 65
slug: /sql-reference/functions/geo/mvt
title: 'Functions for Encoding Mapbox Vector Tiles'
doc_type: 'reference'
---

## Overview {#overview}

[Mapbox Vector Tiles](https://github.com/mapbox/vector-tile-spec) (MVT) are the protobuf-encoded tiles that web map
clients such as MapLibre and Mapbox GL render natively. ClickHouse can build such tiles entirely in SQL with a pair of
cooperating functions:

- `mvtEncodeGeom` — a scalar function that projects a geometry into the tile-local pixel space of a slippy-map tile and
  clips it to the tile.
- `mvtEncode` — an aggregate function that collects the projected geometries of a group into the binary bytes of a
  single-layer tile.

Two helper functions, `mvtTileBBox` and `mvtTileBBoxMercator`, return the bounding box of a tile so that rows can be
restricted to it in the `WHERE` clause using an index.

Point, line and polygon geometry are supported, including the `Geometry` type and the concrete geo types (`Point`,
`LineString`, `MultiLineString`, `Ring`, `Polygon`, `MultiPolygon`).

The resulting bytes are a complete tile that can be returned directly over the HTTP interface with `FORMAT RawBLOB`.

These functions mirror the PostGIS workflow and are also available under their PostGIS names as aliases: `ST_AsMVTGeom`
for `mvtEncodeGeom`, `ST_AsMVT` for `mvtEncode`, and `ST_TileEnvelope` for `mvtTileBBoxMercator`.

## mvtEncodeGeom {#mvtencodegeom}

Projects a geometry given in geographic coordinates (longitude/latitude) into the tile-local pixel space of the
slippy-map tile identified by `zoom`, `tile_x` and `tile_y`, clips it to the tile, snaps it to the integer pixel grid,
and returns the tile-space geometry.

The projection is Web Mercator over the full `UInt32` coordinate range. The returned coordinates have their origin at the
top-left corner of the tile with the y axis pointing downwards, which is the coordinate convention of the Mapbox Vector
Tile format, so the result feeds directly into `mvtEncode`. Coordinates are rounded to whole pixels, so grouping by
`mvtEncodeGeom` collapses geometry falling on the same grid into a single cluster.

When `clip` is enabled (the default), the geometry is clipped to the tile expanded by `buffer` pixels (the range
`[-buffer, extent + buffer]` on each axis); geometry that falls entirely outside becomes `NULL`. This is the analogue of
PostGIS `ST_AsMVTGeom`.

The output geometry type depends on the input: a `Point` returns a `Point`; a `LineString` or `MultiLineString` returns a
`MultiLineString`; a `Ring`, `Polygon` or `MultiPolygon` returns a `MultiPolygon` (clipping may split a geometry into
several parts).

**Syntax**

```sql
mvtEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]])
```

**Arguments**

- `geometry` — Geometry in longitude/latitude degrees. Longitude is clamped to `[-180, 180]` and latitude to the Web Mercator range `[-85.05112878, 85.05112878]`. [`Point`](../../data-types/geo.md) / [`LineString`](../../data-types/geo.md) / [`Polygon`](../../data-types/geo.md) / [`MultiPolygon`](../../data-types/geo.md) / [`Geometry`](../../data-types/geo.md).
- `zoom` — Slippy-map zoom level, in the range `[0, 32]`. [`UInt8`](../../data-types/int-uint.md).
- `tile_x` — Tile column index, in the range `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
- `tile_y` — Tile row index, in the range `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
- `extent` — Optional tile extent in pixels per side. Defaults to `4096`, the Mapbox Vector Tile default. [`UInt32`](../../data-types/int-uint.md).
- `buffer` — Optional clip buffer in pixels. Defaults to `256`. [`UInt32`](../../data-types/int-uint.md).
- `clip` — Optional flag; when nonzero (the default) the geometry is clipped to the tile plus buffer. [`UInt8`](../../data-types/int-uint.md).

**Returned value**

Returns the tile-space geometry, or `NULL` if it is fully clipped out. [`Geometry`](../../data-types/geo.md).

**Example**

```sql
SELECT mvtEncodeGeom((13.37, 52.52)::Point, 10, 550, 335) AS pixel
```

```text
┌─pixel──────┐
│ (124,3384) │
└────────────┘
```

## mvtEncode {#mvtencode}

Encodes a group of features into a binary Mapbox Vector Tile layer. This is the aggregate counterpart of the scalar
function `mvtEncodeGeom`. Each input row becomes one feature; point, line and polygon geometry are supported.

The `geometry` argument is a `Geometry` of tile-space coordinates, typically produced by `mvtEncodeGeom`. Rows whose
geometry is `NULL` (for example, clipped out by `mvtEncodeGeom`) are skipped. The optional `properties` argument is a
named tuple whose element names become the feature attribute keys and whose element types determine the vector tile value
types.

The result is the raw bytes of a single-layer tile. An empty group produces an empty tile. This is the analogue of
PostGIS `ST_AsMVT`.

**Syntax**

```sql
mvtEncode(layer_name[, extent])(geometry[, properties])
```

**Parameters**

- `layer_name` — Name of the vector tile layer. [`String`](../../data-types/string.md).
- `extent` — Tile extent in pixels per side. Defaults to `4096`. [`UInt32`](../../data-types/int-uint.md).

**Arguments**

- `geometry` — Tile-space geometry, for example from `mvtEncodeGeom`. [`Geometry`](../../data-types/geo.md).
- `properties` — Optional named tuple of feature attributes. Element names become attribute keys. [`Tuple`](../../data-types/tuple.md).

**Returned value**

Returns the binary contents of a single-layer Mapbox Vector Tile. [`String`](../../data-types/string.md).

### Property types {#property-types}

Each property element is encoded as the Mapbox Vector Tile `Value` variant matching its ClickHouse type:

| ClickHouse type                          | Vector tile value type |
|------------------------------------------|------------------------|
| `String` / `FixedString`                 | `string_value`         |
| `Float32`                                | `float_value`          |
| `Float64`                                | `double_value`         |
| `Bool`                                   | `bool_value`           |
| `Int8` / `Int16` / `Int32` / `Int64` / `Date32` | `sint_value`    |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` / `Date` / `DateTime` | `uint_value` |

Types may be wrapped in `Nullable` and/or `LowCardinality`. A `NULL` value omits that attribute for the feature, as the
vector tile format has no null. Any other property type raises an exception.

Identical property values are interned into the layer's shared value pool, so a value that appears on many features is
stored only once.

### Naming the properties tuple {#naming-the-properties-tuple}

The properties tuple must have explicit element names. Column aliases inside `tuple(...)` are **not** propagated to tuple
element names, so name the elements with a cast:

```sql
tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)
```

### Clustering {#clustering}

Clustering is expressed in SQL, not by the function. Because `mvtEncodeGeom` rounds to whole pixels, grouping on the
pixel geometry merges coincident geometry; aggregate the group in a subquery, then pass one row per cluster to
`mvtEncode`:

```sql
SELECT mvtEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)) AS tile
FROM
(
    SELECT mvtEncodeGeom((lon, lat)::Point, 10, 550, 335) AS geom, count() AS cluster_count
    FROM points
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1;
```

Grouping on a `Geometry` value requires `allow_suspicious_types_in_group_by = 1`, because grouping by the `Variant`-based
`Geometry` type is restricted by default. Omit the inner `GROUP BY` (and `count()`) to emit one feature per input row
instead of clustered features.

## mvtTileBBox {#mvttilebbox}

Returns the geographic bounding box of the slippy-map tile identified by `zoom`, `tile_x` and `tile_y` as a tuple
`(min_lon, min_lat, max_lon, max_lat)` in degrees.

Use it to restrict rows to a tile while filtering on the `longitude`/`latitude` columns directly — so a primary key or
index on those columns can be used — instead of recomputing the Web Mercator projection per row. The optional `margin`
expands the box on every side by that fraction of the tile size; set it to `buffer / extent` to cover the clip buffer of
`mvtEncodeGeom`.

**Syntax**

```sql
mvtTileBBox(zoom, tile_x, tile_y[, margin])
```

**Arguments**

- `zoom` — Slippy-map zoom level, in the range `[0, 32]`. [`UInt8`](../../data-types/int-uint.md).
- `tile_x` — Tile column index, in the range `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
- `tile_y` — Tile row index, in the range `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
- `margin` — Optional fraction of the tile size to expand the box on every side. Defaults to `0`. [`Float64`](../../data-types/float.md).

**Returned value**

Returns the tile bounding box as a tuple `(min_lon, min_lat, max_lon, max_lat)` in degrees. [`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md).

**Example**

```sql
SELECT mvtTileBBox(0, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────────────────────────┐
│ (-180,-85.05112877980659,180,85.05112877980659)  │
└──────────────────────────────────────────────────┘
```

## mvtTileBBoxMercator {#mvttilebboxmercator}

The Web Mercator counterpart of `mvtTileBBox`, also available under its PostGIS name `ST_TileEnvelope`. Returns the
bounding box of the tile in the full-`UInt32` Web Mercator coordinate space used internally by `mvtEncodeGeom`, as a tuple
`(min_x, min_y, max_x, max_y)`. The y axis grows downward (north at the top). Intended for tables that materialize
Mercator coordinate columns and index those instead of `longitude`/`latitude`.

**Syntax**

```sql
mvtTileBBoxMercator(zoom, tile_x, tile_y[, margin])
```

**Arguments**

Same as [`mvtTileBBox`](#mvttilebbox).

**Returned value**

Returns the tile bounding box as a tuple `(min_x, min_y, max_x, max_y)` in Web Mercator coordinates. [`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md).

**Example**

```sql
SELECT mvtTileBBoxMercator(1, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────┐
│ (0,0,2147483648,2147483648)  │
└──────────────────────────────┘
```

## Restricting rows to a tile {#restricting-rows-to-a-tile}

A tile must only contain the geometry that belongs to it. This is best expressed as two cooperating steps: a cheap,
index-using bounding-box predicate in the `WHERE` clause (performance), and the clip of `mvtEncodeGeom` (correctness).
The clip drops geometry outside the tile, so even a loose bounding-box predicate cannot leak out-of-tile geometry into
the result.

```sql
WITH mvtTileBBox({z:UInt8}, {x:UInt32}, {y:UInt32}, 0.0625) AS bb   -- margin = 256 / 4096
SELECT mvtEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64))
FROM
(
    SELECT mvtEncodeGeom((lon, lat)::Point, {z:UInt8}, {x:UInt32}, {y:UInt32}) AS geom, count() AS cluster_count
    FROM points
    WHERE lon BETWEEN bb.1 AND bb.3 AND lat BETWEEN bb.2 AND bb.4   -- index-using prefilter
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1
```

The bounding-box predicate is only a coarse prefilter; the exact tile boundary is enforced by the clip of
`mvtEncodeGeom`. Pass `clip => false` (the seventh argument) to `mvtEncodeGeom` to disable clipping and rely on the
`WHERE` predicate alone.

## Serving tiles over HTTP {#serving-tiles-over-http}

ClickHouse does not expose a tile endpoint by default: the HTTP interface only accepts queries at `/`. A clean
`/tile/{z}/{x}/{y}` URL is added by the operator with a [predefined query handler](/interfaces/http) in the
server configuration. The handler's `url` uses the `regex:` form to capture the path segments, binds them to query
parameters, and returns the bytes with `FORMAT RawBLOB`:

```xml
<http_handlers>
    <rule>
        <methods>GET</methods>
        <url><![CDATA[regex:/tile/(?P<z>\d+)/(?P<x>\d+)/(?P<y>\d+)]]></url>
        <handler>
            <type>predefined_query_handler</type>
            <query>
                WITH mvtTileBBox({z:UInt8}, {x:UInt32}, {y:UInt32}, 0.0625) AS bb
                SELECT mvtEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64))
                FROM
                (
                    SELECT mvtEncodeGeom((lon, lat)::Point, {z:UInt8}, {x:UInt32}, {y:UInt32}) AS geom, count() AS cluster_count
                    FROM points
                    WHERE lon BETWEEN bb.1 AND bb.3 AND lat BETWEEN bb.2 AND bb.4
                    GROUP BY geom
                )
                SETTINGS allow_suspicious_types_in_group_by = 1
                FORMAT RawBLOB
            </query>
            <content_type>application/vnd.mapbox-vector-tile</content_type>
        </handler>
    </rule>
    <defaults/>
</http_handlers>
```

A `GET /tile/10/550/335` then returns the encoded tile. Omit the inner `GROUP BY` and `count()` for unclustered tiles.

## Limitations {#limitations}

- The Web Mercator projection clamps latitude to `±85.05112878°` and does not handle antimeridian-crossing inputs.
- No explicit feature `id` field.
