-- GeoJSON input format: `MultiPoint` and `GeometryCollection` handling.
--
-- Neither type can be represented in ClickHouse's `Geometry` type. By default
-- (`input_format_geojson_unsupported_geometry_handling = 'throw'`) a feature with such a geometry is
-- rejected; with `= 'null'` it is validated and stored as a NULL geometry. Validation is recursive
-- (a `GeometryCollection` may nest), and a malformed or degenerate member is rejected even in the
-- `'null'` mode, so the choice is only "unrepresentable-but-valid -> NULL" versus "throw" and never
-- lets malformed data through.

-- MultiPoint is unrepresentable: rejected by default, stored as NULL when opted in.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[1,2],[3,4]]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }
SELECT variantType(geometry) FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[1,2],[3,4]]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- GeometryCollection is unrepresentable: rejected by default, stored as NULL when opted in.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }
SELECT variantType(geometry) FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- An empty GeometryCollection is valid (RFC 7946 allows an empty `geometries` array).
SELECT variantType(geometry) FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- A nested GeometryCollection is validated recursively and stored as NULL.
SELECT variantType(geometry) FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- A GeometryCollection whose child is itself an unrepresentable but valid MultiPoint is still NULL.
SELECT variantType(geometry) FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"MultiPoint","coordinates":[[1,2],[3,4]]}]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

-- Even in the `'null'` mode a malformed GeometryCollection is rejected rather than silently stored as NULL:
-- a missing `geometries` member, a `coordinates` member (which belongs only to a coordinate geometry),
-- a non-array `geometries`, a scalar or `null` member, or a duplicate `type` in a child.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection"},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[],"coordinates":[1,2]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":{}},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[1]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[null]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"Point","type":"Point","coordinates":[1,2]}]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }

-- `format_geojson_validate_geometry` still applies to the members of a GeometryCollection stored as NULL:
-- a degenerate child (a LineString with one position, or an unclosed Polygon ring) is rejected by default
-- but accepted when geometry validation is disabled, in which case the whole feature becomes NULL.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"LineString","coordinates":[[0,0]]}]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[2,2]]]}]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null'; -- { serverError INCORRECT_DATA }
SELECT variantType(geometry) FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[{"type":"LineString","coordinates":[[0,0]]}]},"properties":{}}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null', format_geojson_validate_geometry = 0;

-- The representation policy applies only when the geometry is materialized. When the `geometry` column is
-- not selected the unrepresentable type is validation-only, so a MultiPoint feature is accepted even under
-- the default `'throw'` policy (only the JSON grammar and shape are checked, nothing is stored).
SELECT count() FROM format(GeoJSON, 'id Nullable(String)', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[1,2]]},"properties":{}}]}');
