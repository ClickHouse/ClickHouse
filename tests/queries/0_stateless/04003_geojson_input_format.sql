-- Tests for the GeoJSON input format.

-- Basic FeatureCollection with a Point geometry.
SELECT id, geometry, JSONExtractString(properties, 'name') AS name
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [13.4050, 52.5200]},
            "properties": {"name": "Berlin"}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {"type": "Point", "coordinates": [2.3522, 48.8566]},
            "properties": {"name": "Paris"}
        }
    ]
}');

-- Polygon geometry.
SELECT id, variantType(geometry) AS geo_type
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]
            },
            "properties": {}
        }
    ]
}');

-- LineString geometry.
SELECT variantType(geometry) AS geo_type, geometry.LineString
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[0,0],[1,2],[3,4]]}, "properties": {}}
    ]
}');

-- MultiLineString geometry.
SELECT variantType(geometry) AS geo_type, geometry.MultiLineString
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiLineString", "coordinates": [[[0,0],[1,1]],[[2,2],[3,3]]]}, "properties": {}}
    ]
}');

-- Missing id should produce empty string; null geometry should produce None variant.
SELECT id = '', variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": null,
            "properties": {"key": "value"}
        }
    ]
}');

-- Empty features array produces zero rows (tested via INSERT since format() with 0 rows is a known ClickHouse limitation).
-- async_insert causes the client to disconnect after a FORMAT-data INSERT in multiquery mode, so disable it here.
SET async_insert = 0;
CREATE TEMPORARY TABLE __geojson_empty_test (id String, geometry Geometry, properties JSON);
INSERT INTO __geojson_empty_test FORMAT GeoJSON {"type":"FeatureCollection","features":[]};
SELECT count() FROM __geojson_empty_test;

-- Key ordering: coordinates before type.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"coordinates": [1.0, 2.0], "type": "Point"},
            "properties": {}
        }
    ]
}');

-- Schema inference returns the fixed schema.
DESCRIBE format('GeoJSON', '{"type":"FeatureCollection","features":[]}');

-- GeometryCollection: default behaviour is to throw.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "GeometryCollection",
                "geometries": [
                    {"type": "Point", "coordinates": [0, 0]}
                ]
            },
            "properties": {}
        }
    ]
}'); -- { serverError INCORRECT_DATA }

-- GeometryCollection: with null handling inserts NULL for geometry.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "GeometryCollection",
                "geometries": [
                    {"type": "Point", "coordinates": [0, 0]}
                ]
            },
            "properties": {}
        }
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- MultiPoint cannot be represented in the Geometry type: it throws by default instead of silently dropping data.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [[0, 0], [1, 1]]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- MultiPoint with null handling inserts NULL for geometry.
SELECT isNull(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [[0, 0], [1, 1]]}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- A duplicate fixed field within a feature is rejected as bad input.
SELECT id
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "id": "2", "geometry": null, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A 'geometry' column that is not of type Geometry is rejected instead of causing undefined behaviour.
SELECT *
FROM format('GeoJSON', 'id String, geometry String, properties JSON', '{"type":"FeatureCollection","features":[]}'); -- { serverError BAD_ARGUMENTS }

-- A 'geometry' column that is only a partial Geometry Variant is rejected (would otherwise lose data as NULL).
SELECT *
FROM format('GeoJSON', 'geometry Variant(Point), properties JSON', '{"type":"FeatureCollection","features":[]}'); -- { serverError BAD_ARGUMENTS }

-- An unsupported column is rejected (would otherwise produce inconsistently sized columns).
SELECT *
FROM format('GeoJSON', 'geometry Geometry, extra Int32', '{"type":"FeatureCollection","features":[]}'); -- { serverError BAD_ARGUMENTS }

-- An unknown or misspelled geometry type is malformed input and is always rejected, even with null handling.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Piont", "coordinates": [0, 0]}, "properties": {}}
    ]
}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A supported geometry type without a 'coordinates' member is malformed input.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point"}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A missing comma between features is rejected instead of being silently accepted.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{}} {"type":"Feature","geometry":null,"properties":{}}]}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- 'Ring' is part of ClickHouse's Geometry type but is not a valid GeoJSON geometry type, so it is rejected.
SELECT variantType(geometry)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Ring", "coordinates": [[0, 0], [1, 1]]}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- 'geometry' is a required member of a feature: a feature without it is rejected.
SELECT id
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- 'properties' is a required member of a feature: a feature without it is rejected.
SELECT id
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": "1", "geometry": null}
    ]
}'); -- { serverError INCORRECT_DATA }

-- Trailing data after the top-level FeatureCollection object is rejected.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[]} garbage'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Malformed JSON inside an ignored field is rejected (missing comma between members of a skipped object).
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","bbox":{"a":1 "b":2},"features":[]}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- An explicit "properties": null is preserved as NULL (not silently defaulted).
SELECT isNull(properties)
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": null, "properties": null}
    ]
}');

