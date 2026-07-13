-- Duplicate fixed members within a geometry object are rejected, like the duplicate fixed members of
-- a Feature, so that a later value cannot silently override an earlier one.

-- A duplicate 'coordinates' member is rejected.
SELECT geometry.Point
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0],"coordinates":[1,1]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A duplicate 'coordinates' member is rejected even when the first value is malformed, so a later
-- valid value cannot mask the malformed one.
SELECT geometry.Point
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[+Inf,0],"coordinates":[1,1]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A duplicate 'type' member is rejected.
SELECT geometry.Point
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","type":"Point","coordinates":[1,1]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A duplicate 'geometries' member is rejected, shown with a GeometryCollection that would otherwise
-- be accepted as NULL under the 'null' handling setting.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[],"geometries":[]},"properties":{}}]}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A geometry with a single 'coordinates' member is still accepted.
SELECT geometry.Point
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1]},"properties":{}}]}');
