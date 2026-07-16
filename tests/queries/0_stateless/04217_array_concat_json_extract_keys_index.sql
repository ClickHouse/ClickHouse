-- Test for https://github.com/ClickHouse/ClickHouse/issues/88558
-- Multiple index conditions over the same `arrayConcat` expression used to throw
-- `NOT_FOUND_COLUMN_IN_BLOCK`. Fixed by https://github.com/ClickHouse/ClickHouse/pull/94515.

DROP TABLE IF EXISTS t_88558_json;

CREATE TABLE t_88558_json
(
    `attribute` String,
    `resource`  LowCardinality(String),
    `scope`     LowCardinality(String),
    INDEX attribute_keys arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource)) TYPE set(100)
)
ENGINE = MergeTree
ORDER BY (resource)
AS SELECT '{"cluster":"b","c":1}', '{"d":"e","job":"g"}', '{}';

SELECT * FROM t_88558_json
WHERE has(arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource)), 'cluster')
  AND has(arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource)), 'job');

SELECT * FROM t_88558_json
WHERE has(arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource)), 'cluster');

SELECT * FROM t_88558_json
WHERE has(arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource)), 'cluster')
  AND has(arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(resource), JSONExtractKeys(scope)), 'job');

DROP TABLE t_88558_json;

DROP TABLE IF EXISTS t_88558_map;

CREATE TABLE t_88558_map
(
    `attribute` Map(LowCardinality(String), String),
    `resource`  Map(LowCardinality(String), String),
    `scope`     Map(LowCardinality(String), String),
    INDEX attribute_keys arrayConcat(mapKeys(attribute), mapKeys(scope), mapKeys(resource)) TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (resource)
AS SELECT map('cluster', 'b', 'c', '1'), map('d', 'e', 'job', 'g'), map();

-- Wrap each `Map` column in `mapSort` so the output is deterministic regardless of
-- the on-disk map serialization version (`map_serialization_version_for_zero_level_parts`),
-- which CI randomizes (`with_buckets` reorders keys by hash bucket).
SELECT mapSort(attribute), mapSort(resource), mapSort(scope) FROM t_88558_map
WHERE has(arrayConcat(mapKeys(attribute), mapKeys(scope), mapKeys(resource)), 'cluster')
  AND has(arrayConcat(mapKeys(attribute), mapKeys(scope), mapKeys(resource)), 'job');

DROP TABLE t_88558_map;
