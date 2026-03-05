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
SETTINGS input_format_geojson_geometry_collection_handling = 'null';

