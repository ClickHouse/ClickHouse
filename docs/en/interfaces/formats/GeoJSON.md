---
alias: []
description: 'Input format that reads a GeoJSON FeatureCollection and produces one row per feature with id, geometry, and properties columns.'
input_format: true
output_format: false
keywords: ['GeoJSON']
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
| `id`         | `String` | The feature's top-level `id` field, or an empty string if absent.                          |
| `geometry`   | `Geometry`        | The feature's geometry, stored as a `Geometry` variant type.                                |
| `properties` | `JSON`            | The feature's `properties` object, stored as a semi-structured `JSON` column.              |

The `Geometry` type is a `Variant` that can hold `Point`, `LineString`, `Polygon`, `MultiPolygon`, `MultiLineString`, or `Ring`. The `geometry` column is `NULL` when the feature's geometry is `null` or when the geometry type cannot be mapped to a supported variant (e.g. `GeometryCollection` — see [Handling GeometryCollection objects](#geometry-collection) below).

Extra keys in the `FeatureCollection` object (such as `name` or `crs`) and extra keys inside each `Feature` object (such as `type` or `bbox`) are ignored.

Key ordering within geometry objects is flexible: `coordinates` may appear before or after `type`.

Schema inference returns the fixed schema above, so `DESCRIBE` and `SELECT ... FROM format(...)` work without a table definition.

## Example usage {#example-usage}

Given the following GeoJSON file `cities.geojson`:

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [13.4050, 52.5200]},
            "properties": {"name": "Berlin", "population": 3645000}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {"type": "Point", "coordinates": [2.3522, 48.8566]},
            "properties": {"name": "Paris", "population": 2161000}
        }
    ]
}
```

We can query the file like this:

```sql title="Query"
SELECT id, geometry, properties.name AS name
FROM file('cities.geojson', GeoJSON);
```

```response title="Response"
┌─id─┬─geometry─────────┬─name───┐
│ 1  │ (13.405,52.52)   │ Berlin │
│ 2  │ (2.3522,48.8566) │ Paris  │
└────┴──────────────────┴────────┘
```

The file extension `geojson` will be automatically read using the `GeoJSON` format, so you can also exclude the `GeoJSON` format:

```sql title="Query"
SELECT id, geometry, properties.name AS name
FROM file('cities.geojson');
```

This will result in the same output.

We can also ingest GeoJSON data into a table:

```sql title="Query"
CREATE TABLE cities
(
    id         String,
    geometry   Geometry,
    properties JSON,
    name       String MATERIALIZED properties.name
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO cities
SELECT id, geometry, properties
FROM file('cities.geojson', GeoJSON);
```

We can then query that table:

```sql
SELECT *, name 
FROM cities;
```

```response title="Response"
┌─id─┬─geometry─────────┬─properties─────────────────────────────┬─name───┐
│ 1  │ (13.405,52.52)   │ {"name":"Berlin","population":3645000} │ Berlin │
│ 2  │ (2.3522,48.8566) │ {"name":"Paris","population":2161000}  │ Paris  │
└────┴──────────────────┴────────────────────────────────────────┴────────┘
```

We can also infer the schema of GeoJSON data without a table definition:

```sql title="Query"
DESCRIBE format(GeoJSON, '{"type":"FeatureCollection","features":[]}');
```

```response title="Response"
┌─name───────┬─type─────┐
│ id         │ String   │
│ geometry   │ Geometry │
│ properties │ JSON     │
└────────────┴──────────┘
```

## Handling GeometryCollection objects {#geometry-collection}

One of the GeoJSON objects is `GeometryCollection`, which can't be represented by ClickHouse's `Geometry` type.  You can control what happens if `GeometryCollection` is encountered using the `input_format_geojson_geometry_collection_handling` setting. Possible values are:

* `'throw'` — throw an exception (default)
* `'null'` — insert a `NULL` value for the `geometry` column and continue parsing
