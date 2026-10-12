-- The `id` column inferred for the GeoJSON input format is Nullable(String): a feature with an
-- absent or explicit `"id": null` member becomes NULL, while an explicit empty-string id is kept as
-- '' (distinct from NULL). A numeric id is stored verbatim as text. This mirrors `properties`, which
-- is Nullable(JSON) so that a `"properties": null` is preserved.

-- Schema inference yields Nullable(String) for `id`.
DESCRIBE format('GeoJSON', '{"type":"FeatureCollection","features":[]}');

-- An absent id becomes NULL.
SELECT id IS NULL
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{}}]}');

-- An explicit "id": null becomes NULL.
SELECT id IS NULL
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","id":null,"geometry":null,"properties":{}}]}');

-- An explicit empty-string id is kept as '' (NOT NULL).
SELECT id IS NULL, id = ''
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"","geometry":null,"properties":{}}]}');

-- A string id is preserved.
SELECT id
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","id":"abc","geometry":null,"properties":{}}]}');

-- A numeric id is stored verbatim as text.
SELECT id
FROM format('GeoJSON', '{"type":"FeatureCollection","features":[{"type":"Feature","id":7,"geometry":null,"properties":{}}]}');

-- A user-provided non-nullable `id String` column is still accepted (an absent id becomes '').
SELECT id IS NULL, id = ''
FROM format('GeoJSON', 'id String', '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{}}]}');
