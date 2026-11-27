-- A Feature's fixed members are unique: a duplicate 'type' member is rejected, like a duplicate 'id',
-- 'geometry', or 'properties' member, so that a later value cannot silently override an earlier one.

-- A duplicate 'type' member in a Feature is rejected, even when both values are 'Feature'.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","type":"Feature","id":"1","geometry":null,"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A Feature with a single 'type' member is still accepted.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":null,"properties":{}}]}');
