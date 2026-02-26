-- Tests for the experimental GeoJSON input format.
SET allow_experimental_geojson_format = 1;

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

-- Missing id and null geometry should produce defaults.
SELECT isNull(id), variantType(geometry)
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

-- Empty features array.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[]}');

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

-- Format is gated behind the setting.
SET allow_experimental_geojson_format = 0;
SELECT * FROM format('GeoJSON', '{"type":"FeatureCollection","features":[]}'); -- { serverError SUPPORT_IS_DISABLED }
