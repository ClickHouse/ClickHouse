-- `-0.0 = 0.0`, but a `bloom_filter` index holds the hash of each value's bits, which differ for the two
-- zeros. A lookup of a zero must not skip the granule holding the other zero. Each query compares the
-- result with the index against the result without it.
-- https://github.com/ClickHouse/ClickHouse/issues/123744

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_bf_signed_zero;
CREATE TABLE t_bf_signed_zero
(
    id UInt64,
    f Float64,
    g Float32,
    a Array(Float64),
    m Map(Float64, String),
    INDEX idx_f f TYPE bloom_filter GRANULARITY 1,
    INDEX idx_g g TYPE bloom_filter GRANULARITY 1,
    INDEX idx_a a TYPE bloom_filter GRANULARITY 1,
    INDEX idx_m mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_bf_signed_zero VALUES (1, -0.0, -0.0, [-0.0], map(-0.0, 'x')), (2, 0.5, 0.5, [0.5], map(0.5, 'x')), (3, 0.0, 0.0, [0.0], map(0.0, 'x'));

SELECT 'f = 0', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE f = 0 SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE f = 0;
SELECT 'f = -0.0', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE f = -0.0 SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE f = -0.0;
SELECT 'g = 0', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE g = 0 SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE g = 0;
SELECT 'has([0.0, 0.5], f)', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE has([0.0, 0.5], f) SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE has([0.0, 0.5], f);
SELECT 'has(a, 0)', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE has(a, 0) SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE has(a, 0);
SELECT 'hasAny(a, [0.0])', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE hasAny(a, [0.0]) SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE hasAny(a, [0.0]);
SELECT 'hasAll(a, [-0.0])', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE hasAll(a, [-0.0]) SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE hasAll(a, [-0.0]);
SELECT 'mapContainsKey(m, 0.0)', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE mapContainsKey(m, 0.0) SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE mapContainsKey(m, 0.0);
-- With `optimize_functions_to_subcolumns = 0`, `m[0.0]` on a `Map(Float64, String)` throws `ILLEGAL_TYPE_OF_ARGUMENT`
-- even without an index, so the setting is pinned for the queries that use it.
SET optimize_functions_to_subcolumns = 1;
SELECT 'm[0.0] = \'x\'', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE m[0.0] = 'x' SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE m[0.0] = 'x';
-- The key of `m[k]` is looked up by equality, also when the value is compared with `IN`.
SELECT 'm[0.0] IN (\'x\')', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE m[0.0] IN ('x') SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE m[0.0] IN ('x');
-- `IN` compares bits, with and without the index.
SELECT 'f IN (0.0)', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE f IN (0.0) SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE f IN (0.0);
SELECT 'f = 0.5', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero WHERE f = 0.5 SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero WHERE f = 0.5;

SET optimize_functions_to_subcolumns = DEFAULT;

DROP TABLE t_bf_signed_zero;

-- A `mapValues` index: `mapContainsValue` looks up both zeros, `IN` on the value compares bits.
DROP TABLE IF EXISTS t_bf_signed_zero_map_values;
CREATE TABLE t_bf_signed_zero_map_values (id UInt64, mv Map(String, Float64), INDEX idx_mv mapValues(mv) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_bf_signed_zero_map_values VALUES (1, map('a', -0.0)), (2, map('a', 0.5)), (3, map('a', 0.0));

SELECT 'mapContainsValue(mv, 0.0)', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero_map_values WHERE mapContainsValue(mv, 0.0) SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero_map_values WHERE mapContainsValue(mv, 0.0);
SET optimize_functions_to_subcolumns = 1;
SELECT 'mv[\'a\'] IN (0.0)', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_signed_zero_map_values WHERE mv['a'] IN (0.0) SETTINGS use_skip_indexes = 0) FROM t_bf_signed_zero_map_values WHERE mv['a'] IN (0.0);

SET optimize_functions_to_subcolumns = DEFAULT;

DROP TABLE t_bf_signed_zero_map_values;

-- A zero is looked up as two hashes, so the index still skips most granules, for it and for other values.
DROP TABLE IF EXISTS t_bf_signed_zero_pruning;
CREATE TABLE t_bf_signed_zero_pruning (id UInt64, f Float64, INDEX idx_f f TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_bf_signed_zero_pruning SELECT number, number FROM numbers(1000);

SELECT 'pruned for 0', count() FROM t_bf_signed_zero_pruning WHERE f = 0 SETTINGS force_data_skipping_indices = 'idx_f', max_rows_to_read = 500;
SELECT 'pruned for 42', count() FROM t_bf_signed_zero_pruning WHERE f = 42 SETTINGS force_data_skipping_indices = 'idx_f', max_rows_to_read = 500;

DROP TABLE t_bf_signed_zero_pruning;
