---
alias: []
description: 'Input and output format for GeoJSON FeatureCollection documents: on input, one row per feature with id, geometry, and properties columns; on output, one feature per row.'
input_format: true
output_format: true
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[GeoJSON](https://geojson.org/) data is exchanged as a single [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3) document, which ClickHouse maps to three columns — `id`, `geometry`, and `properties` — one set per `Feature`. [Reading](#reading-data) a document produces one row per feature; [writing](#writing-data) produces one feature per row.

## Reading data {#reading-data}

Reading a `FeatureCollection` produces one row per feature with the following fixed schema:

| Column       | Type     | Description                                                                                 |
|--------------|----------|---------------------------------------------------------------------------------------------|
| `id`         | `Nullable(String)` | The feature's `id` member (a JSON string or number), stored as text; `NULL` if the `id` is absent or `null`, while an explicit empty-string id is kept as `''`.                          |
| `geometry`   | `Geometry`        | The feature's geometry, stored as a `Geometry` variant type.                                |
| `properties` | `Nullable(JSON)`  | The feature's `properties` object, stored as a semi-structured `JSON` column. An explicit `"properties": null` is preserved as `NULL`. |

Each geometry is stored in ClickHouse's `Geometry` type (a `Variant`). The supported GeoJSON geometry types are `Point`, `LineString`, `MultiLineString`, `Polygon`, and `MultiPolygon`. The two other GeoJSON geometry types, `GeometryCollection` and `MultiPoint`, cannot be represented by the `Geometry` type; reading one into the `geometry` column raises an exception by default, which can be changed to insert `NULL` instead — see [Handling unsupported geometry types](#unsupported-geometry) below. By default, the `geometry` column is `NULL` only when a feature's geometry is an explicit JSON `null`; under `input_format_geojson_unsupported_geometry_handling = 'null'` it is also `NULL` for an unsupported geometry type.

The document's structure is validated: the top-level `type` must be `FeatureCollection` and every element of `features` must have `type` `Feature`. By default, coordinates must satisfy the GeoJSON shape invariants — a `LineString` (and each line of a `MultiLineString`) must have at least two points, and a `Polygon` ring (and each ring of a `MultiPolygon`) must be closed and have at least four points (see [Geometry validation](#geometry-validation)). Malformed documents are rejected rather than silently loaded.

Key ordering is flexible: the top-level `type` may appear before or after the `features` array, and within a geometry object `coordinates` may appear before or after `type`.

Schema inference returns the fixed schema above, so `DESCRIBE` and `SELECT ... FROM format(...)` work without a table definition.

Given the following GeoJSON file `london.geojson` containing a mix of geometry types:

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

We can query the file and inspect geometry types:

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

The file extension `.geojson` is automatically detected, so the format argument can be omitted:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson');
```

We can use `variantType` to check the underlying type of each Geometry object:

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

And we can extract the underlying data like this:

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

Accessing a `Geometry` subcolumn returns the value when the row holds that type, and the type's default otherwise — `(0,0)` for `Point` and `[]` for the array-based types — so use `variantType(geometry)` to tell which one is set.

We can also ingest GeoJSON data into a table:

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

Then query by feature type:

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

We can also infer the schema of GeoJSON data without a table definition:

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

### Handling unsupported geometry types {#unsupported-geometry}

Some valid GeoJSON geometry types &mdash; such as `GeometryCollection` and `MultiPoint` &mdash; can't be represented by ClickHouse's `Geometry` type. You can control what happens when such a geometry must be stored in the `geometry` column using the `input_format_geojson_unsupported_geometry_handling` setting. Possible values are:

* `'throw'` — throw an exception (default)
* `'null'` — insert a `NULL` value for the `geometry` column and continue parsing

This handling applies only when the `geometry` column is read. When `geometry` is not a requested output column (for example `SELECT id FROM ...`), an unsupported geometry is still validated for well-formedness but does not trigger the handling — it neither throws nor inserts `NULL`, because no geometry value is materialized.

### Limitations {#reading-limitations}

Reading reflects only what fits the fixed schema, so some GeoJSON information is not preserved:

- Only `id`, `geometry`, and `properties` are produced; other document structure is not exposed as columns.
- A position's third (elevation) coordinate, and any beyond it, are dropped — positions become `[longitude, latitude]`.
- `bbox` and foreign members (such as a top-level `name` or `crs`, or extra members inside a `Feature`) are ignored.
- A numeric `id` is stored as text, so the string-vs-number distinction is lost; an absent or `null` `id` becomes `NULL`.
- `GeometryCollection` and `MultiPoint` cannot be represented — see [Handling unsupported geometry types](#unsupported-geometry).

## Writing data {#writing-data}

Writing a result set produces a single GeoJSON [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3), one `Feature` per row.

The columns of the result are mapped onto each `Feature` as follows:

| Feature member | Built from                       | Notes                                                                                                                                                                                                                                                            |
|----------------|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`         | —                                | Always `"Feature"`.                                                                                                                                                                                                                                              |
| `geometry`     | the single geometry-typed column | Exactly one geometry-typed column is required, otherwise the query is rejected. A `NULL` geometry is written as `null`.                                                                                                                                          |
| `id`           | a column named `id`              | Omitted when the value is `NULL`. A `String` column is written as a JSON string, a numeric column as a JSON number.                                                                                                                                              |
| `properties`   | all remaining columns            | A single column named `properties` whose type is object-like (`JSON`, `Map`, or a named `Tuple`) is written directly as the `properties` object instead of being nested under a `properties` key. Otherwise each remaining column becomes one property keyed by its name (an empty object when there are none). |

The geometry-typed column may be the `Geometry` variant or a specific geo type; each maps to a GeoJSON geometry type:

| ClickHouse type   | GeoJSON `"type"`            |
|-------------------|----------------------------|
| `Point`           | `Point`                    |
| `LineString`      | `LineString`               |
| `MultiLineString` | `MultiLineString`          |
| `Polygon`         | `Polygon`                  |
| `MultiPolygon`    | `MultiPolygon`             |
| `Ring`            | `Polygon` (a single ring)  |
| `Geometry`        | the active variant's type (or `null`) |

`Ring` is not a GeoJSON geometry type — a [linear ring](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6) is a component of a `Polygon` — so a `Ring` value is written as a single-ring `Polygon`.

### Examples {#writing-examples}

Continuing with the `london` table [created above](#reading-data), exporting plain attribute columns turns every column other than `id` and `geometry` into a property:

```sql title="Query"
SELECT id, geometry, name, feature_type
FROM london
ORDER BY id
FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London","feature_type":"landmark"}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"name":"River Thames","feature_type":"river"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"name":"Hyde Park","feature_type":"park"}}]}
```

Because a lone object-typed column named `properties` is written out directly, reading a GeoJSON file and writing it straight back reproduces the document (the `id`, `geometry`, and `properties` columns are the ones inferred for the file):

```sql title="Query"
SELECT * FROM file('london.geojson', GeoJSON) FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"feature_type":"landmark","name":"Tower of London","year_built":1078}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"feature_type":"river","length_km":346,"name":"River Thames"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"area_km2":1.42,"feature_type":"park","name":"Hyde Park"}}]}
```

A numeric `id` column is written as a JSON number (a `Nullable` `id` that is `NULL` is omitted entirely):

```sql title="Query"
SELECT 42 AS id, (-0.1276, 51.5072)::Point AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":42,"geometry":{"type":"Point","coordinates":[-0.1276,51.5072]},"properties":{}}]}
```

A `Ring` is written as a single-ring `Polygon`:

```sql title="Query"
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,0]]]},"properties":{}}]}
```

### Writing to a file {#writing-to-a-file}

Use `INTO OUTFILE` to write a GeoJSON file from the client:

```sql title="Query"
SELECT id, geometry, properties
FROM london
ORDER BY id
INTO OUTFILE 'london_export.geojson'
FORMAT GeoJSON;
```

The server can write the file itself with the `file` table function (the `.geojson` extension selects the format automatically):

```sql title="Query"
INSERT INTO FUNCTION file('london_export.geojson', GeoJSON)
SELECT id, geometry, properties FROM london;
```

### Limitations {#writing-limitations}

:::note
ClickHouse's geo types carry no coordinate reference system, so the output assumes coordinates are already WGS84 longitude/latitude in `[longitude, latitude]` order, as [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-4) requires. No reprojection or axis swap is performed, so projected coordinates — or data stored as `(latitude, longitude)` — produce structurally valid but non-conformant GeoJSON.
:::

The output reflects only what ClickHouse stores:

- Information dropped when reading — a position's elevation, `bbox`, foreign members, and an `id`'s string-vs-number distinction — cannot be reproduced; see [Reading limitations](#reading-limitations).
- Coordinates are written from `Float64` values using their shortest round-trippable representation.
- A `properties` object taken directly from a `JSON` column is emitted in the `JSON` type's canonical key order, which may differ from the input.

Geometries are written exactly as stored — coordinate order and winding are preserved. By default, GeoJSON shape validity is enforced on write (see [Geometry validation](#geometry-validation)): a geometry that is not a valid GeoJSON shape, such as a `LineString` with one point or an unclosed `Polygon` ring, is rejected so that the written document reads back. Set `format_geojson_validate_geometry = 0` to emit such geometries as-is instead, producing structurally valid but non-conformant GeoJSON. The right-hand-rule (winding) invariant is not enforced either way, and the distinction between a `null` and an empty `properties` object is preserved.

## Geometry validation {#geometry-validation}

The setting `format_geojson_validate_geometry` controls whether the format enforces [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1) geometry shape rules, in both directions. It is enabled by default.

When enabled, a geometry that violates the GeoJSON shape rules is rejected: a `LineString` (or a line of a `MultiLineString`) with fewer than two points; a `Polygon` or `MultiPolygon` ring with fewer than four points, or whose first and last points differ (an unclosed ring); or an empty `MultiLineString`, `Polygon`, or `MultiPolygon`. The same rules apply when reading such a document and when writing such a ClickHouse value, so a written document always reads back.

When disabled, these shape rules are not enforced in either direction: degenerate geometries are read as-is and written as-is. This lets ClickHouse geometry values that are not valid GeoJSON geometries round-trip through the format, at the cost of producing documents that are not valid GeoJSON.

The validation is structural only: it checks point counts and ring closure. It does not inspect the geometric correctness of a shape, so a structurally valid but geometrically degenerate geometry is accepted in either direction — for example a zero-area polygon, a self-intersecting ring, or a polygon whose holes (inner rings) lie outside its outer ring. The right-hand-rule (winding) orientation of polygon rings is likewise never enforced.

One check is independent of the setting: non-finite coordinates (`NaN`, `Inf`) are always rejected, because they cannot be represented as JSON numbers.
