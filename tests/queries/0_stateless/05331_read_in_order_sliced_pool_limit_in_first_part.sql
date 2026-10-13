-- Tags: no-parallel-replicas
-- The sliced pool is used for local reading only.

-- A query whose rows all lie in the first part in key order must not read the parts after it, however
-- many threads it has and however many slices of the first part come back empty: the pool reads ahead
-- into parts the merge has not started only once the merge went through a part to its end.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET read_in_order_two_level_merge_threshold = 100;
SET use_query_condition_cache = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET materialize_statistics_on_insert = 0;
SET max_block_size = 1024;
-- Slices of at most 4 marks, so that a part is read as many slices.
SET merge_tree_min_rows_for_concurrent_read = 512;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET max_threads = 8;

DROP TABLE IF EXISTS t_sliced_first_part;

CREATE TABLE t_sliced_first_part (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_sliced_first_part;

-- Eight parts with disjoint key ranges of 20000 keys each.
INSERT INTO t_sliced_first_part SELECT number, number * 7 FROM numbers(0, 20000);
INSERT INTO t_sliced_first_part SELECT number, number * 7 FROM numbers(20000, 20000);
INSERT INTO t_sliced_first_part SELECT number, number * 7 FROM numbers(40000, 20000);
INSERT INTO t_sliced_first_part SELECT number, number * 7 FROM numbers(60000, 20000);
INSERT INTO t_sliced_first_part SELECT number, number * 7 FROM numbers(80000, 20000);
INSERT INTO t_sliced_first_part SELECT number, number * 7 FROM numbers(100000, 20000);
INSERT INTO t_sliced_first_part SELECT number, number * 7 FROM numbers(120000, 20000);
INSERT INTO t_sliced_first_part SELECT number, number * 7 FROM numbers(140000, 20000);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_first_part' AND active;

-- The one matching row is the last row of the first part: every other slice of the part comes back
-- empty, and the whole part is cut into slices before the row reaches the merge.
SELECT 'last row of the first part', k FROM t_sliced_first_part PREWHERE v = 19999 * 7 ORDER BY k LIMIT 1 SETTINGS log_comment = '05331_first_part';
SELECT 'last row of the first part', k FROM t_sliced_first_part PREWHERE v = 19999 * 7 ORDER BY k LIMIT 1 SETTINGS read_in_order_use_sliced_pool = 0;

SYSTEM FLUSH LOGS query_log;

-- Rows read: the first part (20000 rows) at most.
SELECT 'within the first part', read_rows <= 20000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05331_first_part'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_sliced_first_part;
