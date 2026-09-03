-- A 'coordinates' member belongs only to a coordinates-based geometry. On a GeometryCollection it is
-- rejected rather than buffered and ignored, mirroring the rejection of a 'geometries' member on a
-- coordinates-based geometry.

-- A GeometryCollection with a stray 'coordinates' member is rejected under the 'null' setting, where
-- the malformed value would otherwise be ignored and the row stored as NULL.
SELECT isNull(geometry)
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,1]}],"coordinates":[+Inf,0]},"properties":{}}]}')
SETTINGS input_format_geojson_unsupported_geometry_handling='null'; -- { serverError INCORRECT_DATA }

-- A valid GeometryCollection without a 'coordinates' member is still accepted as NULL under the setting.
SELECT isNull(geometry)
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,1]}]},"properties":{}}]}')
SETTINGS input_format_geojson_unsupported_geometry_handling='null';
