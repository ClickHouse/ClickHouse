DROP TABLE IF EXISTS t_lc_single_dictionary_external_two_level_buckets;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE t_lc_single_dictionary_external_two_level_buckets
(
    part UInt8,
    k LowCardinality(String),
    u16 LowCardinality(UInt16)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_lc_single_dictionary_external_two_level_buckets;

INSERT INTO t_lc_single_dictionary_external_two_level_buckets
SELECT 0, toString(number), toUInt16(number)
FROM numbers(16)
SETTINGS low_cardinality_use_single_dictionary_for_part = 1;

INSERT INTO t_lc_single_dictionary_external_two_level_buckets
SELECT 1, toString((number * 7) % 16), toUInt16((number * 7) % 16)
FROM numbers(16)
SETTINGS low_cardinality_use_single_dictionary_for_part = 1;

SELECT throwIf(
    count() != 2 OR countIf(part_type = 'Wide') != 2,
    'Expected two active Wide parts')
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_lc_single_dictionary_external_two_level_buckets'
  AND active;

SELECT count(), countIf(c != 2), countIf(arraySort(parts) != [0, 1]), sum(c)
FROM
(
    SELECT k, count() AS c, groupArray(part) AS parts
    FROM t_lc_single_dictionary_external_two_level_buckets
    GROUP BY k
)
SETTINGS
    max_threads = 1,
    max_block_size = 16,
    optimize_read_in_order = 0,
    group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 0,
    max_bytes_before_external_group_by = 1,
    max_bytes_ratio_before_external_group_by = 0;

SELECT count(), countIf(c != 2), countIf(arraySort(parts) != [0, 1]), sum(c)
FROM
(
    SELECT u16, count() AS c, groupArray(part) AS parts
    FROM t_lc_single_dictionary_external_two_level_buckets
    GROUP BY u16
)
SETTINGS
    max_threads = 1,
    max_block_size = 16,
    optimize_read_in_order = 0,
    group_by_two_level_threshold = 1,
    group_by_two_level_threshold_bytes = 0,
    max_bytes_before_external_group_by = 1,
    max_bytes_ratio_before_external_group_by = 0,
    log_comment = '05026_low_cardinality_uint16_no_external_spill';

-- `low_cardinality_key16` has no two-level form. It must not first aggregate by dictionary
-- index and then spill normalized single-level blocks whose `bucket_num = -1` would make the
-- external merge rebuild the whole table at once.
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0 AND max(ProfileEvents['ExternalAggregationWritePart']) = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '05026_low_cardinality_uint16_no_external_spill'
  AND type = 'QueryFinish';

DROP TABLE t_lc_single_dictionary_external_two_level_buckets;
