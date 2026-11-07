-- A `bloom_filter` index keeps pruning for `hasAll` with a zero element and for `arrayJoin(col) = constant`
-- over float elements. A zero is looked up as both `0.0` and `-0.0`, which compare equal but hash apart.
-- Each query compares the result with the index against the result without it.

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_bf_zero_follow_up;
CREATE TABLE t_bf_zero_follow_up
(
    id UInt64,
    a Array(Float64),
    b Array(Float32),
    INDEX idx_a a TYPE bloom_filter GRANULARITY 1,
    INDEX idx_b b TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_bf_zero_follow_up SELECT number, [toFloat64(number)], [toFloat32(number) / 10] FROM numbers(100);
INSERT INTO t_bf_zero_follow_up VALUES (100, [-0.0, 42.0], [0.5]), (101, [0.0, 42.0], [-0.0]), (102, [42.0], [0.1]);

SELECT 'hasAll(a, [0.0, 42.0])', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE hasAll(a, [0.0, 42.0]) SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE hasAll(a, [0.0, 42.0]);
SELECT 'hasAll(a, [-0.0])', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE hasAll(a, [-0.0]) SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE hasAll(a, [-0.0]);
SELECT 'arrayJoin(a) = 0.0', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE arrayJoin(a) = 0.0 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE arrayJoin(a) = 0.0;
SELECT 'arrayJoin(a) = 42', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE arrayJoin(a) = 42 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE arrayJoin(a) = 42;
SELECT 'arrayJoin(b) = 0', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE arrayJoin(b) = 0 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE arrayJoin(b) = 0;
SELECT 'arrayJoin(b) = 0.5', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE arrayJoin(b) = 0.5 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE arrayJoin(b) = 0.5;
-- `0.1` is not exactly a `Float32`, so the constant is not converted and the index is not used.
SELECT 'arrayJoin(b) = 0.1', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE arrayJoin(b) = 0.1 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE arrayJoin(b) = 0.1;
-- Both zeros, or the same zero several times, in the constant array.
SELECT 'hasAny(a, [0.0, -0.0])', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE hasAny(a, [0.0, -0.0]) SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE hasAny(a, [0.0, -0.0]);
SELECT 'hasAny(a, [-0.0, -0.0, 42.0])', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE hasAny(a, [-0.0, -0.0, 42.0]) SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE hasAny(a, [-0.0, -0.0, 42.0]);
SELECT 'hasAll(a, [0.0, -0.0, 42.0])', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE hasAll(a, [0.0, -0.0, 42.0]) SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE hasAll(a, [0.0, -0.0, 42.0]);
SELECT 'hasAll(a, [0.0, 0.0])', arraySort(groupArray(id)), (SELECT arraySort(groupArray(id)) FROM t_bf_zero_follow_up WHERE hasAll(a, [0.0, 0.0]) SETTINGS use_skip_indexes = 0) FROM t_bf_zero_follow_up WHERE hasAll(a, [0.0, 0.0]);

-- The index skips most granules.
SELECT 'pruned for hasAll(a, [0.0, 42.0])', count() FROM t_bf_zero_follow_up WHERE hasAll(a, [0.0, 42.0]) SETTINGS force_data_skipping_indices = 'idx_a', max_rows_to_read = 30;
SELECT 'pruned for hasAll(a, [0.0, -0.0, 42.0])', count() FROM t_bf_zero_follow_up WHERE hasAll(a, [0.0, -0.0, 42.0]) SETTINGS force_data_skipping_indices = 'idx_a', max_rows_to_read = 30;
SELECT 'pruned for hasAny(a, [0.0, -0.0])', count() FROM t_bf_zero_follow_up WHERE hasAny(a, [0.0, -0.0]) SETTINGS force_data_skipping_indices = 'idx_a', max_rows_to_read = 30;
SELECT 'pruned for hasAny(a, [-0.0, -0.0, 42.0])', count() FROM t_bf_zero_follow_up WHERE hasAny(a, [-0.0, -0.0, 42.0]) SETTINGS force_data_skipping_indices = 'idx_a', max_rows_to_read = 30;
SELECT 'pruned for arrayJoin(a) = 42', count() FROM t_bf_zero_follow_up WHERE arrayJoin(a) = 42 SETTINGS force_data_skipping_indices = 'idx_a', max_rows_to_read = 30;
SELECT 'pruned for arrayJoin(a) = 0.0', count() FROM t_bf_zero_follow_up WHERE arrayJoin(a) = 0.0 SETTINGS force_data_skipping_indices = 'idx_a', max_rows_to_read = 30;
SELECT 'pruned for arrayJoin(b) = 0.5', count() FROM t_bf_zero_follow_up WHERE arrayJoin(b) = 0.5 SETTINGS force_data_skipping_indices = 'idx_b', max_rows_to_read = 30;

DROP TABLE t_bf_zero_follow_up;

-- `LEFT ARRAY JOIN` pads an empty array with `+0.0`, which equals `-0.0` too. A granule with an empty and a
-- nonempty array without zero holds neither zero hash, so a zero constant must not use the index.
DROP TABLE IF EXISTS t_bf_zero_padded;
CREATE TABLE t_bf_zero_padded
(
    id UInt64,
    a Array(Float64),
    b Array(Float32),
    INDEX idx_a a TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX idx_b b TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_bf_zero_padded SELECT number, if(number % 2 = 0, [], [1.0]), if(number % 2 = 0, [], [1.0]) FROM numbers(100);

SELECT 'LEFT ARRAY JOIN a, x = -0.0', count(), (SELECT count() FROM t_bf_zero_padded LEFT ARRAY JOIN a AS x WHERE x = -0.0 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_padded LEFT ARRAY JOIN a AS x WHERE x = -0.0 SETTINGS use_query_condition_cache = 0;
SELECT 'LEFT ARRAY JOIN a, x = 0.0', count(), (SELECT count() FROM t_bf_zero_padded LEFT ARRAY JOIN a AS x WHERE x = 0.0 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_padded LEFT ARRAY JOIN a AS x WHERE x = 0.0 SETTINGS use_query_condition_cache = 0;
SELECT 'LEFT ARRAY JOIN b, x = -0.0', count(), (SELECT count() FROM t_bf_zero_padded LEFT ARRAY JOIN b AS x WHERE x = -0.0 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_padded LEFT ARRAY JOIN b AS x WHERE x = -0.0 SETTINGS use_query_condition_cache = 0;
SELECT 'LEFT ARRAY JOIN a, x = 1', count(), (SELECT count() FROM t_bf_zero_padded LEFT ARRAY JOIN a AS x WHERE x = 1 SETTINGS use_skip_indexes = 0) FROM t_bf_zero_padded LEFT ARRAY JOIN a AS x WHERE x = 1 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_bf_zero_padded;
