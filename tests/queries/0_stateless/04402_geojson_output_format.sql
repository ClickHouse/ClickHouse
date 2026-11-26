-- The GeoJSON output format writes a single FeatureCollection, one Feature per row.

-- Each concrete geometry type, as a standalone typed column.
SELECT (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT [(0., 0.), (1., 1.), (2., 2.)]::LineString AS geometry FORMAT GeoJSON;
SELECT [[(0., 0.), (1., 1.)], [(2., 2.), (3., 3.)]]::MultiLineString AS geometry FORMAT GeoJSON;
SELECT [[(0., 0.), (10., 0.), (10., 10.), (0., 0.)]]::Polygon AS geometry FORMAT GeoJSON;
SELECT [[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]]::MultiPolygon AS geometry FORMAT GeoJSON;
-- A `Ring` has no GeoJSON type, so it is emitted as a single-ring Polygon.
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;

-- The `Geometry` Variant dispatches on the active type. `Polygon` and `MultiLineString` share the
-- same storage but are distinguished by type; a NULL geometry is emitted as `null`.
SELECT geometry FROM format(GeoJSON, '{"type":"FeatureCollection","features":[
    {"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},"properties":{}},
    {"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]},"properties":{}},
    {"type":"Feature","geometry":{"type":"MultiLineString","coordinates":[[[0,0],[1,1]]]},"properties":{}},
    {"type":"Feature","geometry":null,"properties":{}}
]}') FORMAT GeoJSON;

-- id: a string id, a numeric-column id (emitted as a JSON number), omitted for a NULL id, and a
-- genuine empty-string id.
SELECT 'x' AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT 42::UInt32 AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT CAST(NULL AS Nullable(String)) AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT '' AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;

-- properties: built from the remaining columns, empty when there are none, or splatted directly from
-- a lone object-typed column named `properties` (a NULL of which becomes `null`).
SELECT (1.0, 2.0)::Point AS geometry, 'Berlin' AS name, 37::UInt32 AS pop FORMAT GeoJSON;
SELECT (1.0, 2.0)::Point AS geometry FORMAT GeoJSON;
SELECT (1.0, 2.0)::Point AS geometry, '{"a":1,"b":"x"}'::JSON AS properties FORMAT GeoJSON;
SELECT (1.0, 2.0)::Point AS geometry, CAST(NULL AS Nullable(JSON)) AS properties FORMAT GeoJSON;

-- Multiple rows become comma-separated features; zero rows produce an empty FeatureCollection.
SELECT toString(number) AS id, (toFloat64(number), toFloat64(number))::Point AS geometry FROM numbers(3) FORMAT GeoJSON;
SELECT (1.0, 2.0)::Point AS geometry WHERE 0 FORMAT GeoJSON;

-- Round-trip: reading a FeatureCollection and re-emitting it reproduces id, geometry, and properties.
SELECT * FROM format(GeoJSON, '{"type":"FeatureCollection","features":[
    {"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London"}}
]}') FORMAT GeoJSON;

-- Errors: there must be exactly one geometry-typed column. The output format is constructed
-- client-side for `SELECT ... FORMAT`, so these surface as client errors.
SELECT 1 AS x FORMAT GeoJSON; -- { clientError BAD_ARGUMENTS }
SELECT (1.0, 2.0)::Point AS a, (3.0, 4.0)::Point AS b FORMAT GeoJSON; -- { clientError BAD_ARGUMENTS }
