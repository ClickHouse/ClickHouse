---
alias: []
description: 'Input format that reads a GeoJSON FeatureCollection and produces one row per feature with id, geometry, and properties columns.'
input_format: true
output_format: false
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

Reads a [GeoJSON](https://geojson.org/) `FeatureCollection` document and produces one row per feature. Each row has the following fixed schema:

| Column       | Type     | Description                                                                                 |
|--------------|----------|---------------------------------------------------------------------------------------------|
| `id`         | `String` | The feature's `id` member (a JSON string or number), stored as text; an empty string if the `id` is absent or `null`.                          |
| `geometry`   | `Geometry`        | The feature's geometry, stored as a `Geometry` variant type.                                |
| `properties` | `Nullable(JSON)`  | The feature's `properties` object, stored as a semi-structured `JSON` column. An explicit `"properties": null` is preserved as `NULL`. |

Each geometry is stored in ClickHouse's `Geometry` type (a `Variant`). The supported GeoJSON geometry types are `Point`, `LineString`, `MultiLineString`, `Polygon`, and `MultiPolygon`. The two other GeoJSON geometry types, `GeometryCollection` and `MultiPoint`, cannot be represented by the `Geometry` type; reading one into the `geometry` column raises an exception by default, which can be changed to insert `NULL` instead — see [Handling unsupported geometry types](#unsupported-geometry) below. By default, the `geometry` column is `NULL` only when a feature's geometry is an explicit JSON `null`; under `input_format_geojson_unsupported_geometry_handling = 'null'` it is also `NULL` for an unsupported geometry type.

The document's structure is validated: the top-level `type` must be `FeatureCollection` and every element of `features` must have `type` `Feature`. Coordinates must satisfy the GeoJSON shape invariants — a `LineString` (and each line of a `MultiLineString`) must have at least two positions, and a `Polygon` ring (and each ring of a `MultiPolygon`) must be closed and have at least four positions. Malformed documents are rejected rather than silently loaded.

Other keys in the `FeatureCollection` object (such as `name` or `crs`) and other keys inside each `Feature` object (such as `bbox`) are ignored.

Key ordering is flexible: the top-level `type` may appear before or after the `features` array, and within a geometry object `coordinates` may appear before or after `type`.

Schema inference returns the fixed schema above, so `DESCRIBE` and `SELECT ... FROM format(...)` work without a table definition.

## Example usage {#example-usage}

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
┌─name───────┬─type───────────┐
│ id         │ String         │
│ geometry   │ Geometry       │
│ properties │ Nullable(JSON) │
└────────────┴────────────────┘
```

## Handling unsupported geometry types {#unsupported-geometry}

Some valid GeoJSON geometry types &mdash; such as `GeometryCollection` and `MultiPoint` &mdash; can't be represented by ClickHouse's `Geometry` type. You can control what happens when such a geometry must be stored in the `geometry` column using the `input_format_geojson_unsupported_geometry_handling` setting. Possible values are:

* `'throw'` — throw an exception (default)
* `'null'` — insert a `NULL` value for the `geometry` column and continue parsing

This handling applies only when the `geometry` column is read. When `geometry` is not a requested output column (for example `SELECT id FROM ...`), an unsupported geometry is still validated for well-formedness but does not trigger the handling — it neither throws nor inserts `NULL`, because no geometry value is materialized.
