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

To access the `Point` geometry's coordinates directly, filter on `variantType`:

```sql title="Query"
SELECT properties.name AS name, geometry
FROM file('london.geojson', GeoJSON)
WHERE variantType(geometry) = 'Point';
```

```response title="Response"
┌─name────────────┬─geometry──────────┐
│ Tower of London │ (-0.0761,51.5081) │
└─────────────────┴───────────────────┘
```

We can also ingest GeoJSON data into a table:

```sql title="Query"
CREATE TABLE london
(
    id           String,
    geometry     Geometry,
    properties   JSON,
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
