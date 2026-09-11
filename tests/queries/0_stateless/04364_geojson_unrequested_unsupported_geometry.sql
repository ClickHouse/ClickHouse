-- An unrequested 'geometry' member is validated for well-formedness but not materialized, so the
-- throw/NULL representation policy for an unsupported geometry type applies only when the geometry
-- column is actually selected.

-- Selecting only 'id' over a feature with a valid MultiPoint succeeds under the default 'throw'
-- handling, because no geometry value is written.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"m","geometry":{"type":"MultiPoint","coordinates":[[1,1]]},"properties":{}}]}');

-- A malformed unsupported geometry is still rejected even when the geometry column is not selected.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"m","geometry":{"type":"MultiPoint","coordinates":[[+Inf,1]]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- Selecting the geometry column still throws for an unsupported type under the default handling.
SELECT geometry
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[1,1]]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }
