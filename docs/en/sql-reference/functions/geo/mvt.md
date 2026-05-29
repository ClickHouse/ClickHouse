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

- `mvtEncodeGeom` — a scalar function that projects a geographic point into the tile-local pixel space of a slippy-map
  tile.
- `mvtEncode` — an aggregate function that collects the projected points of a group into the binary bytes of a
  single-layer tile, optionally clipping features to the tile.

Two helper functions, `mvtTileBBox` and `mvtTileBBoxMercator`, return the bounding box of a tile so that rows can be
restricted to it in the `WHERE` clause using an index.

The resulting bytes are a complete tile that can be returned directly over the HTTP interface with `FORMAT RawBLOB`.

Only point geometry is supported.

## mvtEncodeGeom {#mvtencodegeom}

Projects a geographic point given by `longitude` and `latitude` into the tile-local pixel space of the slippy-map tile
identified by `zoom`, `tile_x` and `tile_y`, and returns the pixel coordinates.

The projection is Web Mercator over the full `UInt32` coordinate range. The returned coordinates have their origin at the
top-left corner of the tile with the y axis pointing downwards, which is the coordinate convention of the Mapbox Vector
Tile format, so the result feeds directly into `mvtEncode`. Coordinates are rounded to whole pixels, so grouping by
`mvtEncodeGeom` collapses all points falling on the same pixel into a single cluster.

The function transforms point geometry only and does not clip to the tile boundary; restrict rows to the tile in the
`WHERE` clause (see [Restricting rows to a tile](#restricting-rows-to-a-tile)) and, if needed, drop points just outside
the tile with the `buffer` parameter of `mvtEncode`.

**Syntax**

```sql
mvtEncodeGeom(longitude, latitude, zoom, tile_x, tile_y[, extent])
```

**Arguments**

- `longitude` — Longitude in degrees. Values are clamped to the range `[-180, 180]`. [`Float64`](../../data-types/float.md).
- `latitude` — Latitude in degrees. Values are clamped to the Web Mercator range `[-85.05112878, 85.05112878]`. [`Float64`](../../data-types/float.md).
- `zoom` — Slippy-map zoom level, in the range `[0, 32]`. [`UInt8`](../../data-types/int-uint.md).
- `tile_x` — Tile column index, in the range `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
- `tile_y` — Tile row index, in the range `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
- `extent` — Optional tile extent in pixels per side. Defaults to `4096`, the Mapbox Vector Tile default. [`UInt32`](../../data-types/int-uint.md).

**Returned value**

Returns the tile-local pixel coordinates as a tuple `(pixel_x, pixel_y)`. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Example**

```sql
SELECT mvtEncodeGeom(13.37, 52.52, 10, 550, 335) AS pixel
```

```text
┌─pixel──────┐
│ (124,3384) │
└────────────┘
```

## mvtEncode {#mvtencode}

Encodes a group of point features into a binary Mapbox Vector Tile layer. This is the aggregate counterpart of the
scalar function `mvtEncodeGeom`. Each input row becomes one point feature.

The `geometry` argument must be a tuple of tile-space pixel coordinates `(pixel_x, pixel_y)`, typically produced by
`mvtEncodeGeom`. The optional `properties` argument is a named tuple whose element names become the feature attribute
keys and whose element types determine the vector tile value types.

The result is the raw bytes of a single-layer tile. An empty group produces an empty tile.

When the optional `buffer` parameter is given, the tile is clipped: features whose pixel coordinates fall outside the
tile expanded by `buffer` pixels (the range `[-buffer, extent + buffer]`) are dropped. This enforces the exact tile
boundary while the `WHERE` clause uses a coarse bounding-box prefilter for performance (see
[Restricting rows to a tile](#restricting-rows-to-a-tile)).

**Syntax**

```sql
mvtEncode(layer_name[, extent[, buffer]])(geometry[, properties])
```

**Parameters**

- `layer_name` — Name of the vector tile layer. [`String`](../../data-types/string.md).
- `extent` — Tile extent in pixels per side. Defaults to `4096`. [`UInt32`](../../data-types/int-uint.md).
- `buffer` — Optional clip buffer in pixels. When given, features outside `[-buffer, extent + buffer]` are dropped; when omitted, no clipping is performed. [`UInt32`](../../data-types/int-uint.md).

**Arguments**

- `geometry` — Tile-space pixel coordinates of the point as a tuple `(pixel_x, pixel_y)`, for example from `mvtEncodeGeom`. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).
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
| `Int8` / `Int16` / `Int32` / `Int64`     | `int_value`            |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` | `uint_value`           |
| `Date` / `DateTime`                      | `uint_value` (raw epoch) |

Types may be wrapped in `Nullable` and/or `LowCardinality`. A `NULL` value omits that attribute for the feature, as the
vector tile format has no null. Any other property type raises an exception.

### Naming the properties tuple {#naming-the-properties-tuple}

The properties tuple must have explicit element names. Column aliases inside `tuple(...)` are **not** propagated to tuple
element names, so name the elements with a cast:

```sql
tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)
```

### Clustering {#clustering}

Clustering is expressed in SQL, not by the function. Because `mvtEncodeGeom` rounds to whole pixels, grouping on the
pixel geometry merges coincident points; aggregate the group in a subquery, then pass one row per cluster to `mvtEncode`:

```sql
SELECT mvtEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)) AS tile
FROM
(
    SELECT mvtEncodeGeom(lon, lat, 10, 550, 335) AS geom, count() AS cluster_count
    FROM points
    GROUP BY geom
);
```

Omit the inner `GROUP BY` (and `count()`) to emit one feature per input row instead of clustered features.

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

The Web Mercator counterpart of `mvtTileBBox`. Returns the bounding box of the tile in the full-`UInt32` Web Mercator
coordinate space used internally by `mvtEncodeGeom`, as a tuple `(min_x, min_y, max_x, max_y)`. The y axis grows
downward (north at the top). Intended for tables that materialize Mercator coordinate columns and index those instead of
`longitude`/`latitude`.

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

A tile must only contain the points that belong to it. This is best expressed as two cooperating steps: a cheap,
index-using bounding-box predicate in the `WHERE` clause (performance), and the `buffer` clip parameter of `mvtEncode`
(correctness). The clip drops features outside the tile during aggregation, so even a loose bounding-box
predicate cannot leak out-of-tile points into the result.

```sql
WITH mvtTileBBox({z:UInt8}, {x:UInt32}, {y:UInt32}, 0.0156) AS bb   -- margin = 64 / 4096
SELECT mvtEncode('points', 4096, 64)(geom, tuple(cluster_count)::Tuple(cluster_count UInt64))   -- buffer = 64
FROM
(
    SELECT mvtEncodeGeom(lon, lat, {z:UInt8}, {x:UInt32}, {y:UInt32}) AS geom, count() AS cluster_count
    FROM points
    WHERE lon BETWEEN bb.1 AND bb.3 AND lat BETWEEN bb.2 AND bb.4   -- index-using prefilter
    GROUP BY geom
)
```

Without the `buffer` parameter `mvtEncode` does not clip; in that case the bounding-box predicate alone determines which
points are encoded and must match the tile.

## Serving tiles over HTTP {#serving-tiles-over-http}

ClickHouse does not expose a tile endpoint by default: the HTTP interface only accepts queries at `/`. A clean
`/tile/{z}/{x}/{y}` URL is added by the operator with a [predefined query handler](../../../interfaces/http.md) in the
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
                WITH mvtTileBBox({z:UInt8}, {x:UInt32}, {y:UInt32}, 0.0156) AS bb
                SELECT mvtEncode('points', 4096, 64)(geom, tuple(cluster_count)::Tuple(cluster_count UInt64))
                FROM
                (
                    SELECT mvtEncodeGeom(lon, lat, {z:UInt8}, {x:UInt32}, {y:UInt32}) AS geom, count() AS cluster_count
                    FROM points
                    WHERE lon BETWEEN bb.1 AND bb.3 AND lat BETWEEN bb.2 AND bb.4
                    GROUP BY geom
                )
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

- Point geometry only; line and polygon geometry are not supported. Clipping (the `buffer` parameter of `mvtEncode`)
  is likewise point-only.
- The Web Mercator projection clamps latitude to `±85.05112878°` and does not handle antimeridian-crossing inputs.
- No explicit feature `id` field.
