-- Tags: no-darwin
-- no-darwin: the fast float parser (precise_float_parsing = 0) rounds to different least-significant digits on Darwin.

-- GeoJSON coordinates are parsed as Float64 and must honor precise_float_parsing like other input formats.
-- 4.56 parses to different bits under the two algorithms, so it is a visible discriminator.

SELECT 'precise (default)';
SELECT geometry FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1, 4.56]},"properties":{}}]}');
SELECT 'fast';
SELECT geometry FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1, 4.56]},"properties":{}}]}') SETTINGS precise_float_parsing = 0;
