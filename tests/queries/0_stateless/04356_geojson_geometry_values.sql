-- Tests for GeoJSON geometry values and coordinate/structural edge cases.

-- A Polygon with a hole keeps both the outer ring and the inner ring.
SELECT geometry.Polygon
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[0,0],[10,0],[10,10],[0,10],[0,0]],[[2,2],[4,2],[4,4],[2,4],[2,2]]]}, "properties": {}}
    ]
}');

-- A MultiPolygon keeps every polygon.
SELECT geometry.MultiPolygon
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "MultiPolygon", "coordinates": [[[[0,0],[2,0],[2,2],[0,2],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]]}, "properties": {}}
    ]
}');

-- A position may carry a third elevation value; the position is read and the elevation is dropped.
SELECT geometry.Point
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [13.405, 52.52, 100]}, "properties": {}}
    ]
}');

-- Negative coordinates are read as-is.
SELECT geometry.Point
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-122.4, 37.7]}, "properties": {}}
    ]
}');

-- A Point with an empty coordinate array is malformed.
SELECT geometry
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": []}, "properties": {}}
    ]
}'); -- { serverError INCORRECT_DATA }

-- A non-array 'coordinates' value is malformed.
SELECT geometry
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": 5}, "properties": {}}
    ]
}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- A non-array 'features' value is malformed.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":5}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- A second top-level 'features' array is rejected as a duplicate, rather than silently keeping
-- only the first array and dropping the rest.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{}}],"features":[{"type":"Feature","geometry":null,"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A coordinate with a leading zero is not a valid JSON number and is rejected.
SELECT geometry
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [01, 0]}, "properties": {}}
    ]
}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- A document nested deeper than the configured JSON limit is rejected, even inside a skipped member.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","bbox":[[[[[1]]]]],"features":[]}')
SETTINGS input_format_json_max_depth = 2; -- { serverError TOO_DEEP_RECURSION }

-- An invalid value for the unsupported-geometry-handling setting is rejected.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[]}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'bogus'; -- { clientError BAD_ARGUMENTS }

-- An explicit "id": null is treated like an absent id: the Nullable(String) `id` becomes NULL.
SELECT id IS NULL
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": null, "geometry": null, "properties": {}}
    ]
}');

-- A negative numeric id is stored verbatim.
SELECT id
FROM format('GeoJSON', '{
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": -42, "geometry": null, "properties": {}}
    ]
}');
