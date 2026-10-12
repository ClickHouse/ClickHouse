-- Test for ensuring correct accounting of SelectedMarks and SelectedRanges
-- with use_skip_indexes_on_data_read = 1
-- Tags: no-parallel-replicas : need accurate skip index filtering statistics

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    id UInt64,
    v UInt64,
    INDEX vidx v TYPE minmax
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 64,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns=0;

-- 3 parts
INSERT INTO test SELECT number, number FROM numbers(10000);
INSERT INTO test SELECT number, number FROM numbers(10000);
INSERT INTO test SELECT number, number FROM numbers(10000);

SELECT COUNT(*) FROM test WHERE v < 500 SETTINGS log_comment = 'test_data_read_1';
SELECT COUNT(*) FROM test WHERE v < 500 OR v > 9000 SETTINGS log_comment = 'test_data_read_2';
SELECT COUNT(*) FROM test WHERE v > 900000 SETTINGS log_comment = 'test_data_read_3';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['SelectedMarks'], ProfileEvents['SelectedRanges'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_data_read_1';

SELECT ProfileEvents['SelectedMarks'], ProfileEvents['SelectedRanges'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_data_read_2';

SELECT ProfileEvents['SelectedMarks'], ProfileEvents['SelectedRanges'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='test_data_read_3';

DROP TABLE test;

