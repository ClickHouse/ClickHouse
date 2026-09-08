-- JSONAllPaths skip-index equality coverage for cuckoo_filter and binary_fuse_filter.
--
-- The JSONAllPaths index stores the set of JSON path names present in each granule.
-- For a predicate `json.path = value`, granules where the path is absent can be skipped
-- only when a missing path can never satisfy the predicate:
--   * Dynamic / Nullable path expression: a missing path is NULL, so `NULL = value` is
--     never true -> safe to skip.
--   * Non-nullable path expression (e.g. a CAST to a concrete type): a missing path
--     yields the type default, so `json.path::T = <default>` matches granules where the
--     path is absent and the granule MUST NOT be skipped.
-- This is a false-negative-sensitive branch: a wrong decision would make
-- force_data_skipping_indices drop granules where the path is present or absent.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;

-- { echoOn }

DROP TABLE IF EXISTS t_json_eq_cuckoo;
CREATE TABLE t_json_eq_cuckoo
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE cuckoo_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_json_eq_cuckoo VALUES (1, '{"a": 1}'), (2, '{"a": 2}'), (3, '{"b": 1}'), (4, '{"b": 2}'), (5, '{"a": 1, "b": 9}'), (6, '{"c": 5}'), (7, '{}');

-- Present path, non-default value: index is applicable; the forced result must equal the full scan.
SELECT count() FROM t_json_eq_cuckoo WHERE json.a = 1;
SELECT count() FROM t_json_eq_cuckoo WHERE json.a = 1 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_json_eq_cuckoo WHERE json.a = 1 SETTINGS use_skip_indexes = 0;

-- Absent path: every granule can be skipped, the result is empty either way.
SELECT count() FROM t_json_eq_cuckoo WHERE json.z = 1;
SELECT count() FROM t_json_eq_cuckoo WHERE json.z = 1 SETTINGS force_data_skipping_indices = 'idx';

-- Present path via CAST to a non-nullable type with a non-default value: still safe to use the index.
SELECT count() FROM t_json_eq_cuckoo WHERE json.a::Int64 = 1;
SELECT count() FROM t_json_eq_cuckoo WHERE json.a::Int64 = 1 SETTINGS force_data_skipping_indices = 'idx';

-- Default value on a non-nullable path expression: a missing path produces the default, so the
-- index must NOT prune. The result with the index must equal the result without skip indexes.
SELECT count() FROM t_json_eq_cuckoo WHERE json.a::Int64 = 0;
SELECT count() FROM t_json_eq_cuckoo WHERE json.a::Int64 = 0 SETTINGS use_skip_indexes = 0;

DROP TABLE t_json_eq_cuckoo;

DROP TABLE IF EXISTS t_json_eq_fuse;
CREATE TABLE t_json_eq_fuse
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE binary_fuse_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_json_eq_fuse VALUES (1, '{"a": 1}'), (2, '{"a": 2}'), (3, '{"b": 1}'), (4, '{"b": 2}'), (5, '{"a": 1, "b": 9}'), (6, '{"c": 5}'), (7, '{}');

SELECT count() FROM t_json_eq_fuse WHERE json.a = 1;
SELECT count() FROM t_json_eq_fuse WHERE json.a = 1 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_json_eq_fuse WHERE json.a = 1 SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_json_eq_fuse WHERE json.z = 1;
SELECT count() FROM t_json_eq_fuse WHERE json.z = 1 SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM t_json_eq_fuse WHERE json.a::Int64 = 1;
SELECT count() FROM t_json_eq_fuse WHERE json.a::Int64 = 1 SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM t_json_eq_fuse WHERE json.a::Int64 = 0;
SELECT count() FROM t_json_eq_fuse WHERE json.a::Int64 = 0 SETTINGS use_skip_indexes = 0;

DROP TABLE t_json_eq_fuse;
