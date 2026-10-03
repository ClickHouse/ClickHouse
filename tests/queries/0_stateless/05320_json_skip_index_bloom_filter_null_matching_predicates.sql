-- A `JSONAllPaths` bloom_filter index must not prune granules lacking a path for a predicate that holds on the
-- missing (NULL) value: `IN (NULL, ...)` and `NOT IN` under `transform_null_in = 1`, and `has([..., NULL], path)`.

DROP TABLE IF EXISTS t_json_bf_null;
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS t_json_bf_null;
CREATE TABLE t_json_bf_null (id UInt64, json JSON, INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO t_json_bf_null VALUES (1, '{"a":42}'), (2, '{"a":43}'), (3, '{"b":1}'), (4, '{"b":2}');

SELECT 'IN', count() FROM t_json_bf_null WHERE json.a.:Int64 IN (42, 43) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'IN with NULL', count() FROM t_json_bf_null WHERE json.a.:Int64 IN (NULL, 42) SETTINGS transform_null_in = 1;
SELECT count() FROM t_json_bf_null WHERE json.a.:Int64 IN (NULL, 42) SETTINGS transform_null_in = 1, force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'NOT IN', count() FROM t_json_bf_null WHERE json.a.:Int64 NOT IN (42) SETTINGS transform_null_in = 1;
SELECT count() FROM t_json_bf_null WHERE json.a.:Int64 NOT IN (42) SETTINGS transform_null_in = 1, force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'has with NULL', count() FROM t_json_bf_null WHERE has([42, NULL], json.a.:Int64);
SELECT count() FROM t_json_bf_null WHERE has([42, NULL], json.a.:Int64) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_json_bf_null;
