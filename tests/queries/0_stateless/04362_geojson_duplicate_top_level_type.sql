-- The top-level 'type' member of a FeatureCollection is unique: a duplicate is rejected whether it
-- appears before or after the 'features' array, so a malformed document cannot carry two of them.

-- A duplicate top-level 'type' member after 'features' is rejected, even when both are 'FeatureCollection'.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":null,"properties":{}}],"type":"FeatureCollection"}'); -- { serverError INCORRECT_DATA }

-- A duplicate top-level 'type' member before 'features' is rejected.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":null,"properties":{}}]}'); -- { serverError INCORRECT_DATA }

-- A FeatureCollection with a single 'type' member is still accepted.
SELECT id
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":null,"properties":{}}]}');
