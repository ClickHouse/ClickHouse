DROP TABLE IF EXISTS t_lc_single_dictionary_index;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE t_lc_single_dictionary_index
(
    id UInt64,
    s LowCardinality(String),
    f LowCardinality(FixedString(16)),
    ns LowCardinality(Nullable(String)),
    u128 LowCardinality(UInt128),
    u256 LowCardinality(UInt256)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 64,
    index_granularity_bytes = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_lc_single_dictionary_index;

INSERT INTO t_lc_single_dictionary_index
SELECT
    toUInt64(k) * 100000 + replica AS id,
    concat('value_', toString(k)) AS s,
    CAST(concat('fixed_', leftPad(toString(k), 10, '0')), 'FixedString(16)') AS f,
    CAST(if(k = 0, NULL, concat('nullable_', toString(k))), 'Nullable(String)') AS ns,
    toUInt128(k) AS u128,
    toUInt256(k) AS u256
FROM
(
    SELECT toUInt8(number) AS k
    FROM numbers(16)
)
ARRAY JOIN range((toUInt64(k) + 1) * 128) AS replica
SETTINGS low_cardinality_use_single_dictionary_for_part = 1;

SELECT 'one_part_string', countIf(c != (toUInt64(substring(s, 7)) + 1) * 128), sum(c)
FROM
(
    SELECT s, count() AS c
    FROM t_lc_single_dictionary_index
    GROUP BY s
)
SETTINGS max_threads = 1, max_block_size = 31, group_by_two_level_threshold = 1, optimize_read_in_order = 0;

SELECT 'one_part_uint128', countIf(c != (toUInt64(u128) + 1) * 128), sum(c)
FROM
(
    SELECT u128, count() AS c
    FROM t_lc_single_dictionary_index
    GROUP BY u128
)
SETTINGS max_threads = 1, max_block_size = 31, group_by_two_level_threshold = 1, optimize_read_in_order = 0;

SELECT 'one_part_parallel_string', countIf(c != (toUInt64(substring(s, 7)) + 1) * 128), sum(c)
FROM
(
    SELECT s, count() AS c
    FROM t_lc_single_dictionary_index
    GROUP BY s
)
SETTINGS
    max_threads = 4,
    max_block_size = 31,
    group_by_two_level_threshold = 1,
    merge_tree_min_rows_for_concurrent_read = 1,
    optimize_read_in_order = 0;

INSERT INTO t_lc_single_dictionary_index
SELECT
    2000000 + toUInt64(k) * 100000 + replica AS id,
    concat('value_', toString(k)) AS s,
    CAST(concat('fixed_', leftPad(toString(k), 10, '0')), 'FixedString(16)') AS f,
    CAST(if(k = 0, NULL, concat('nullable_', toString(k))), 'Nullable(String)') AS ns,
    toUInt128(k) AS u128,
    toUInt256(k) AS u256
FROM
(
    SELECT toUInt8((number * 7) % 16) AS k
    FROM numbers(16)
)
ARRAY JOIN range((toUInt64(k) + 1) * 128) AS replica
SETTINGS low_cardinality_use_single_dictionary_for_part = 1;

SELECT 'two_parts_string', countIf(c != (toUInt64(substring(s, 7)) + 1) * 256), sum(c)
FROM
(
    SELECT s, count() AS c
    FROM t_lc_single_dictionary_index
    GROUP BY s
)
SETTINGS max_threads = 1, max_block_size = 31, group_by_two_level_threshold = 1, optimize_read_in_order = 0;

SELECT 'two_parts_top_k', s, count()
FROM t_lc_single_dictionary_index
GROUP BY s
ORDER BY s
LIMIT 3
SETTINGS
    max_threads = 1,
    max_block_size = 31,
    group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 0,
    optimize_read_in_order = 0,
    enable_group_by_top_k_optimization = 1,
    query_plan_max_limit_for_top_k_optimization = 1000;

SELECT 'two_parts_uint256', countIf(c != (toUInt64(u256) + 1) * 256), sum(c)
FROM
(
    SELECT u256, count() AS c
    FROM t_lc_single_dictionary_index
    GROUP BY u256
)
SETTINGS max_threads = 1, max_block_size = 31, group_by_two_level_threshold = 1, optimize_read_in_order = 0;

SELECT 'two_parts_fixed_string', countIf(c != (toUInt64(substring(toString(f), 7)) + 1) * 256), sum(c)
FROM
(
    SELECT f, count() AS c
    FROM t_lc_single_dictionary_index
    GROUP BY f
)
SETTINGS max_threads = 1, max_block_size = 31, group_by_two_level_threshold = 1, optimize_read_in_order = 0;

SELECT
    'two_parts_nullable_string',
    countIf(if(isNull(ns), c != 256, c != (toUInt64(substring(ns, 10)) + 1) * 256)),
    sum(c)
FROM
(
    SELECT ns, count() AS c
    FROM t_lc_single_dictionary_index
    GROUP BY ns
)
SETTINGS max_threads = 1, max_block_size = 31, group_by_two_level_threshold = 1, optimize_read_in_order = 0;

SELECT 'parallel_string', countIf(c != (toUInt64(substring(s, 7)) + 1) * 256), sum(c)
FROM
(
    SELECT s, count() AS c
    FROM t_lc_single_dictionary_index
    GROUP BY s
)
SETTINGS
    max_threads = 4,
    max_block_size = 31,
    group_by_two_level_threshold = 1,
    merge_tree_min_rows_for_concurrent_read = 1,
    optimize_read_in_order = 0;

DROP TABLE t_lc_single_dictionary_index;
