-- A 'geometries' member belongs only to a GeometryCollection. On any other geometry type it is
-- rejected rather than buffered and silently ignored, so malformed content inside it cannot slip
-- through unvalidated.

-- A Point with a 'geometries' member is rejected, even when the member's content is malformed.
SELECT geometry.Point
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1],"geometries":[+Inf]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A Polygon with a 'geometries' member is rejected.
SELECT geometry.Polygon
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]],"geometries":[]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A MultiPoint (stored as NULL under the 'null' setting) with a 'geometries' member is rejected too.
SELECT count()
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[0,0]],"geometries":[]},"properties":{}}]}')
SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- A Point without a 'geometries' member is still accepted.
SELECT geometry.Point
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1]},"properties":{}}]}');
