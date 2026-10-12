-- A compound geometry must contain at least one child: an empty 'coordinates' array (no rings, no
-- lines, or no polygons) is a degenerate geometry and is rejected rather than stored as a non-NULL
-- value, matching the rejection of a degenerate LineString or ring.

-- An empty Polygon (no rings) is rejected.
SELECT geometry.Polygon
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- An empty MultiLineString (no lines) is rejected.
SELECT geometry.MultiLineString
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiLineString","coordinates":[]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- An empty MultiPolygon (no polygons) is rejected.
SELECT geometry.MultiPolygon
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A MultiPolygon that contains a ringless polygon is rejected.
SELECT geometry.MultiPolygon
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[]]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A non-empty Polygon, MultiLineString, and MultiPolygon are still accepted.
SELECT geometry.Polygon
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]},"properties":{}}]}');

SELECT geometry.MultiLineString
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiLineString","coordinates":[[[0,0],[1,1]]]},"properties":{}}]}');

SELECT geometry.MultiPolygon
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]]]},"properties":{}}]}');
