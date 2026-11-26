-- format_geojson_validate_geometry (default 1) makes the GeoJSON format enforce RFC 7946 geometry shape
-- rules in both directions. Setting it to 0 disables them, so degenerate geometries are read and written
-- as-is and round-trip through the format. The default-on output rejection happens mid-stream and is
-- checked by exit code in 04407_geojson_output_value_errors.

-- Reading rejects a degenerate document by default, but accepts it when validation is disabled.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"LineString","coordinates":[[0,0]]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"LineString","coordinates":[[0,0]]},"properties":{}}]}') SETTINGS format_geojson_validate_geometry = 0;
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiLineString","coordinates":[]},"properties":{}}]}') SETTINGS format_geojson_validate_geometry = 0;
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,1]]]},"properties":{}}]}') SETTINGS format_geojson_validate_geometry = 0;
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[2,2]]]},"properties":{}}]}') SETTINGS format_geojson_validate_geometry = 0;

-- Writing emits the same degenerate geometries as-is when validation is disabled.
SELECT [(0.0, 0.0)]::LineString AS geometry FORMAT GeoJSON SETTINGS format_geojson_validate_geometry = 0;
SELECT []::MultiLineString AS geometry FORMAT GeoJSON SETTINGS format_geojson_validate_geometry = 0;
SELECT []::Polygon AS geometry FORMAT GeoJSON SETTINGS format_geojson_validate_geometry = 0;
SELECT [(0.0, 0.0), (1.0, 1.0)]::Ring AS geometry FORMAT GeoJSON SETTINGS format_geojson_validate_geometry = 0;
SELECT [[(0.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]::Polygon AS geometry FORMAT GeoJSON SETTINGS format_geojson_validate_geometry = 0;
SELECT [[]]::MultiPolygon AS geometry FORMAT GeoJSON SETTINGS format_geojson_validate_geometry = 0;
