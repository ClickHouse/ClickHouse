---
alias: []
description: 'Documentation for the GeoJSON format'
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

:::note
This format is experimental. To use it, set [`allow_experimental_geojson_format`](/operations/settings/settings-formats.md/#allow_experimental_geojson_format) to `1`.
:::

Reads a [GeoJSON](https://geojson.org/) `FeatureCollection` document and produces one row per feature. Each row has the following fixed schema:

| Column       | Type              | Description                                                                                 |
|--------------|-------------------|---------------------------------------------------------------------------------------------|
| `id`         | `Nullable(String)` | The feature's top-level `id` field, or `NULL` if absent.                                   |
| `geometry`   | `Geometry`        | The feature's geometry, stored as a `Geometry` variant type.                                |
| `properties` | `JSON`            | The feature's `properties` object, stored as a semi-structured `JSON` column.              |

The `Geometry` type is a `Variant` that can hold `Point`, `LineString`, `Polygon`, `MultiPolygon`, `MultiLineString`, or `Ring`. The `geometry` column is `NULL` when the feature's geometry is `null` or when the geometry type cannot be mapped to a supported variant (e.g. `GeometryCollection` — see [format settings](#format-settings) below).

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

Read its contents directly:

```sql title="Query"
SELECT id, geometry, JSONExtractString(properties, 'name') AS name
FROM file('cities.geojson', GeoJSON)
SETTINGS allow_experimental_geojson_format = 1
```

```response title="Response"
┌─id─┬─geometry─────────┬─name───┐
│ 1  │ (13.405,52.52)   │ Berlin │
│ 2  │ (2.3522,48.8566) │ Paris  │
└────┴──────────────────┴────────┘
```

Insert features into a table:

```sql title="Query"
CREATE TABLE cities
(
    id         Nullable(String),
    geometry   Geometry,
    properties JSON,
    name       String MATERIALIZED JSONExtractString(properties, 'name')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO cities
SELECT id, geometry, properties
FROM file('cities.geojson', GeoJSON)
SETTINGS allow_experimental_geojson_format = 1;
```

Check the schema inferred automatically:

```sql title="Query"
DESCRIBE format(GeoJSON, '{"type":"FeatureCollection","features":[]}')
SETTINGS allow_experimental_geojson_format = 1
```

```response title="Response"
┌─name───────┬─type─────────────┐
│ id         │ Nullable(String) │
│ geometry   │ Geometry         │
│ properties │ JSON             │
└────────────┴──────────────────┘
```

## Format settings {#format-settings}

| Setting                                                    | Description                                                                                                                                                    | Default   |
|------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| `allow_experimental_geojson_format`                        | Enable the `GeoJSON` input format. Must be set to `1` to use this format.                                                                                      | `0`       |
| `input_format_geojson_geometry_collection_handling`        | Controls what happens when a `GeometryCollection` geometry type is encountered. `GeometryCollection` cannot be represented in ClickHouse's `Geometry` type. Possible values: `'throw'` — throw an exception; `'null'` — insert a `NULL` value for the `geometry` column and continue parsing. | `'throw'` |
