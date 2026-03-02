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

Given the following GeoJSON file `berlin.geojson` containing a mix of geometry types:

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [13.4050, 52.5200]},
            "properties": {"name": "Berlin", "feature_type": "city", "population": 3645000}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {
                "type": "LineString",
                "coordinates": [[13.3888, 52.5163], [13.4050, 52.5200], [13.4210, 52.5163]]
            },
            "properties": {"name": "Unter den Linden", "feature_type": "boulevard"}
        },
        {
            "type": "Feature",
            "id": "3",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[13.3500, 52.5116], [13.3800, 52.5116], [13.3800, 52.5200], [13.3500, 52.5200], [13.3500, 52.5116]]]
            },
            "properties": {"name": "Tiergarten", "feature_type": "park", "area_km2": 2.1}
        }
    ]
}
```

We can query the file and inspect geometry types:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('berlin.geojson', GeoJSON);
```

```response title="Response"
┌─id─┬─name─────────────┬─geo_type───┐
│ 1  │ Berlin           │ Point      │
│ 2  │ Unter den Linden │ LineString │
│ 3  │ Tiergarten       │ Polygon    │
└────┴──────────────────┴────────────┘
```

The file extension `.geojson` is automatically detected, so the format argument can be omitted:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('berlin.geojson');
```

To access the `Point` geometry's coordinates directly, filter on `variantType`:

```sql title="Query"
SELECT properties.name AS name, geometry
FROM file('berlin.geojson', GeoJSON)
WHERE variantType(geometry) = 'Point';
```

```response title="Response"
┌─name───┬─geometry───────┐
│ Berlin │ (13.405,52.52) │
└────────┴────────────────┘
```

We can also ingest GeoJSON data into a table:

```sql title="Query"
CREATE TABLE berlin
(
    id           String,
    geometry     Geometry,
    properties   JSON,
    name         String MATERIALIZED properties.name,
    feature_type String MATERIALIZED properties.feature_type
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO berlin
SELECT id, geometry, properties
FROM file('berlin.geojson', GeoJSON);
```

Then query by feature type:

```sql title="Query"
SELECT name, feature_type, variantType(geometry) AS geo_type
FROM berlin
ORDER BY id;
```

```response title="Response"
┌─name─────────────┬─feature_type─┬─geo_type───┐
│ Berlin           │ city         │ Point      │
│ Unter den Linden │ boulevard    │ LineString │
│ Tiergarten       │ park         │ Polygon    │
└──────────────────┴──────────────┴────────────┘
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
