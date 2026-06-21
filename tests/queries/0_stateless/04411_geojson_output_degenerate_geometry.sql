-- The GeoJSON output format serializes ClickHouse geometry values faithfully rather than enforcing
-- RFC 7946 geometry validity. ClickHouse can hold degenerate geometries (a LineString with a single
-- position, an empty MultiLineString, a Polygon ring with fewer than four positions, and so on), and the
-- output emits them as-is. These are not valid GeoJSON geometries, so the strict GeoJSON input format
-- rejects them on read-back: such geometries deliberately do not round-trip. This test documents both
-- the faithful output and that round-trip limitation.

-- Faithful output of degenerate geometries.
SELECT [(0.0, 0.0)]::LineString AS geometry FORMAT GeoJSON;
SELECT []::MultiLineString AS geometry FORMAT GeoJSON;
SELECT []::Polygon AS geometry FORMAT GeoJSON;
SELECT []::MultiPolygon AS geometry FORMAT GeoJSON;
SELECT [(0.0, 0.0), (1.0, 1.0)]::Ring AS geometry FORMAT GeoJSON;
SELECT [[(0.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]::Polygon AS geometry FORMAT GeoJSON;
SELECT [[]]::MultiPolygon AS geometry FORMAT GeoJSON;

-- The strict input format rejects these same documents, confirming the round-trip limitation.
SELECT count() FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"LineString","coordinates":[[0,0]]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }
SELECT count() FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiLineString","coordinates":[]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }
SELECT count() FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,1]]]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }
