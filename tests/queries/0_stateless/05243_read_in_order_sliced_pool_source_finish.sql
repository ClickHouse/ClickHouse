-- Tags: no-parallel-replicas
-- The sliced pool is used for local reading only.

-- Once every part is read, the sources of the sliced pool end their streams like any other source,
-- so the end-of-read work of a source happens: the predicate statistics are logged, and the query
-- condition cache learns about the last slice every source read.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET read_in_order_two_level_merge_threshold = 100;
SET max_block_size = 1024;
-- Slices of 8 marks, so that a part is read as several slices.
SET merge_tree_min_rows_for_concurrent_read = 512;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET use_query_condition_cache = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS t_sliced_finish;

CREATE TABLE t_sliced_finish (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_sliced_finish;

INSERT INTO t_sliced_finish SELECT number, number * 7 FROM numbers(0, 20000);
INSERT INTO t_sliced_finish SELECT number, number * 7 FROM numbers(12000, 20000);
INSERT INTO t_sliced_finish SELECT number, number * 7 FROM numbers(16000, 20000);
INSERT INTO t_sliced_finish SELECT number, number * 7 FROM numbers(30000, 20000);

SELECT 'router in pipeline', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%') > 0
FROM (EXPLAIN PIPELINE SELECT k FROM t_sliced_finish PREWHERE v % 7 = 1 ORDER BY k LIMIT 1000000);

-- No row matches (v is a multiple of 7) and no statistics can tell, so every slice of every part is read to the end.
SELECT 'no match', count() FROM (SELECT k FROM t_sliced_finish PREWHERE v % 7 = 1 ORDER BY k LIMIT 1000000)
SETTINGS predicate_statistics_sample_rate = 1, log_comment = '05243_sliced_pool_first_read';

-- The cache knows that no mark of any part matches, including the marks of the slices read last.
SELECT 'no match again', count() FROM (SELECT k FROM t_sliced_finish PREWHERE v % 7 = 1 ORDER BY k LIMIT 1000000)
SETTINGS log_comment = '05243_sliced_pool_second_read';

SYSTEM FLUSH LOGS query_log, predicate_statistics_log;

SELECT 'read rows', log_comment, read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05243_sliced_pool_first_read', '05243_sliced_pool_second_read')
ORDER BY log_comment;

SELECT 'predicate statistics logged', count() > 0 FROM system.predicate_statistics_log
WHERE database = currentDatabase() AND table = 't_sliced_finish' AND event_date >= yesterday();

DROP TABLE t_sliced_finish;
