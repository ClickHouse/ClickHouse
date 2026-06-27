-- The GeoJSON input format ignores members it does not expose as columns: foreign members, a `bbox` at
-- any level, a position's third and later coordinates, and an unrequested `properties` value. Such
-- ignored numbers are validated only for JSON-number grammar, not for finiteness, so a valid JSON number
-- that merely overflows Float64 (such as 1e400) in an ignored position must not reject the document.

-- An unrequested `properties` value outside the Float64 range is ignored (only `id` is selected).
SELECT id FROM format(GeoJSON, 'id Nullable(String)', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"x","geometry":null,"properties":{"huge":1e400}}]}') FORMAT TSVRaw;

-- A foreign member at the FeatureCollection level.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","foreign":1e400,"features":[{"type":"Feature","id":"x","geometry":null,"properties":{}}]}');

-- A foreign member inside a geometry object.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2],"foreign":1e400},"properties":{}}]}');

-- A position's third coordinate (altitude) is dropped, so an out-of-range value there is ignored.
SELECT geometry FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2,1e400]},"properties":{}}]}') FORMAT GeoJSON;

-- A `bbox` member at the FeatureCollection, Feature, and geometry levels.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","bbox":[0,0,1e400,1],"features":[{"type":"Feature","id":"x","geometry":null,"properties":{}}]}');
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","bbox":[0,0,1e400,1],"id":"x","geometry":null,"properties":{}}]}');
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2],"bbox":[0,0,1e400,1]},"properties":{}}]}');

-- Only finiteness is relaxed for ignored numbers, not the JSON-number grammar, so a non-JSON spelling
-- such as +Inf is still rejected even in an ignored `bbox`.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","bbox":[0,0,+Inf,1],"features":[{"type":"Feature","geometry":null,"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A non-finite value in a stored coordinate is still rejected.
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[1e400,2]},"properties":{}}]}'); -- { serverError INCORRECT_DATA }
