-- JSONAllPaths skip-index `IN` coverage under `transform_null_in` for cuckoo_filter and binary_fuse_filter.
--
-- The JSONAllPaths index stores the set of JSON path names present in each granule, so for
-- `json.path IN (set)` a granule where the path is absent may be skipped only when the value a
-- missing path produces cannot satisfy the predicate:
--   * a path expression that can hold NULL yields NULL, and `NULL IN (NULL, ...)` is false with
--     the default `transform_null_in = 0`, but true with `transform_null_in = 1` — in the latter
--     case a granule without the path MUST NOT be skipped;
--   * a non-nullable path expression (e.g. `CAST` to a concrete type) yields the type default, so
--     `json.path::T IN (<default>)` must not prune granules where the path is absent.
--
-- `IN` is not defined on a bare `Dynamic` argument, so the nullable case is reached through a
-- `CAST` to `Nullable(T)`. This is a false-negative-sensitive branch: with `index_granularity = 1`
-- and `GRANULARITY 1` every row is its own granule, so a wrong pruning decision silently drops
-- matching rows.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;

-- { echoOn }

DROP TABLE IF EXISTS t_json_in_null_cuckoo;
CREATE TABLE t_json_in_null_cuckoo
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE cuckoo_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_json_in_null_cuckoo VALUES (1, '{"a": 1}'), (2, '{"a": 2}'), (3, '{"b": 1}'), (4, '{"b": 2}'), (5, '{"a": 1, "b": 9}'), (6, '{"c": 5}'), (7, '{}');

-- Sets without NULL: the index is applicable for a present path with a non-default value.
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Nullable(Int64) IN (1) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Nullable(Int64) IN (1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Int64 IN (1) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_json_in_null_cuckoo WHERE json.z::Nullable(Int64) IN (1) SETTINGS force_data_skipping_indices = 'idx';

-- A non-nullable path expression compared with the type default: a missing path produces the
-- default, so the index must not prune and the result must equal the full scan.
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Int64 IN (0);
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Int64 IN (0) SETTINGS use_skip_indexes = 0;

-- With the default `transform_null_in = 0` a NULL in the set matches nothing, so pruning is safe.
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Nullable(Int64) IN (1, NULL) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Nullable(Int64) IN (1, NULL) SETTINGS use_skip_indexes = 0;

SET transform_null_in = 1;

-- Now a set holding NULL matches the missing path, so granules without the path must be kept:
-- the result with skip indexes enabled must equal the full scan.
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Nullable(Int64) IN (NULL);
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Nullable(Int64) IN (NULL) SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Nullable(Int64) IN (1, NULL);
SELECT count() FROM t_json_in_null_cuckoo WHERE json.a::Nullable(Int64) IN (1, NULL) SETTINGS use_skip_indexes = 0;

SET transform_null_in = 0;

DROP TABLE t_json_in_null_cuckoo;

DROP TABLE IF EXISTS t_json_in_null_fuse;
CREATE TABLE t_json_in_null_fuse
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE binary_fuse_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_json_in_null_fuse VALUES (1, '{"a": 1}'), (2, '{"a": 2}'), (3, '{"b": 1}'), (4, '{"b": 2}'), (5, '{"a": 1, "b": 9}'), (6, '{"c": 5}'), (7, '{}');

SELECT count() FROM t_json_in_null_fuse WHERE json.a::Nullable(Int64) IN (1) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_json_in_null_fuse WHERE json.a::Nullable(Int64) IN (1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_in_null_fuse WHERE json.a::Int64 IN (1) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_json_in_null_fuse WHERE json.z::Nullable(Int64) IN (1) SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM t_json_in_null_fuse WHERE json.a::Int64 IN (0);
SELECT count() FROM t_json_in_null_fuse WHERE json.a::Int64 IN (0) SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_json_in_null_fuse WHERE json.a::Nullable(Int64) IN (1, NULL) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_json_in_null_fuse WHERE json.a::Nullable(Int64) IN (1, NULL) SETTINGS use_skip_indexes = 0;

SET transform_null_in = 1;

SELECT count() FROM t_json_in_null_fuse WHERE json.a::Nullable(Int64) IN (NULL);
SELECT count() FROM t_json_in_null_fuse WHERE json.a::Nullable(Int64) IN (NULL) SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_json_in_null_fuse WHERE json.a::Nullable(Int64) IN (1, NULL);
SELECT count() FROM t_json_in_null_fuse WHERE json.a::Nullable(Int64) IN (1, NULL) SETTINGS use_skip_indexes = 0;

SET transform_null_in = 0;

DROP TABLE t_json_in_null_fuse;
