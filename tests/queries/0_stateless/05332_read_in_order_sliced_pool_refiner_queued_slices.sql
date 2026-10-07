-- Tags: no-parallel-replicas, no-object-storage
-- The sliced pool is used for local reading only.

-- The first slice a source takes from a lane refines the whole lane with the index. The slices of the
-- lane that were cut before and still wait in the FIFO must be refined with it, not emptied: an
-- emptied slice is dropped without being read and its rows are missing from the result.
-- The parts start at staggered keys, so when the merge asks the lane it is on for its second slice,
-- the lanes behind it get two slices each before anyone has started them. More sources than threads
-- make the source taking the second of them run only after the refinement by the first.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET read_in_order_two_level_merge_threshold = 1000;
SET use_skip_indexes_on_data_read = 1;
SET use_indexes_refiner_in_read_pools = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET query_plan_remove_redundant_sorting = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET materialize_statistics_on_insert = 0;
SET max_block_size = 1024;
SET max_insert_threads = 1;
-- Slices of at most 4 marks of 128 rows: the ramp of a lane is 1 and 2 marks.
SET merge_tree_min_rows_for_concurrent_read = 512;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET merge_tree_min_read_task_size = 1;
SET max_threads = 2;
SET max_streams_to_max_threads_ratio = 8;

DROP TABLE IF EXISTS t_sliced_refine;

CREATE TABLE t_sliced_refine (k UInt64, v Int64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_sliced_refine;

-- 16 parts of 20000 rows; part p holds the keys p * 3000 + 16 * i.
INSERT INTO t_sliced_refine SELECT     0 + 16 * number,     0 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT  3000 + 16 * number,  3000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT  6000 + 16 * number,  6000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT  9000 + 16 * number,  9000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 12000 + 16 * number, 12000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 15000 + 16 * number, 15000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 18000 + 16 * number, 18000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 21000 + 16 * number, 21000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 24000 + 16 * number, 24000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 27000 + 16 * number, 27000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 30000 + 16 * number, 30000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 33000 + 16 * number, 33000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 36000 + 16 * number, 36000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 39000 + 16 * number, 39000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 42000 + 16 * number, 42000 + 16 * number FROM numbers(20000);
INSERT INTO t_sliced_refine SELECT 45000 + 16 * number, 45000 + 16 * number FROM numbers(20000);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_refine' AND active;
SELECT 'router in pipeline', count() FROM (EXPLAIN PIPELINE SELECT count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v >= 0 ORDER BY k LIMIT 1000000)) WHERE explain LIKE '%MergeTreeInOrderSliceRouter%';

-- The index drops nothing: every row must come out. Repeated, since the loss depends on the order the
-- sources run in.
SELECT 'all rows', count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v >= 0 ORDER BY k LIMIT 1000000);
SELECT 'all rows', count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v >= 0 ORDER BY k LIMIT 1000000);
SELECT 'all rows', count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v >= 0 ORDER BY k LIMIT 1000000);
SELECT 'all rows', count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v >= 0 ORDER BY k LIMIT 1000000);
SELECT 'all rows', count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v >= 0 ORDER BY k LIMIT 1000000);

-- The index drops the upper marks of every part; the rest of the queued slices is still read.
SELECT 'index drops marks', count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v < 200000 ORDER BY k LIMIT 1000000);
SELECT 'index drops marks', count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v < 200000 ORDER BY k LIMIT 1000000);
SELECT 'index drops marks', count(), sum(k) FROM (SELECT k FROM t_sliced_refine WHERE v < 200000 ORDER BY k LIMIT 1000000);

DROP TABLE t_sliced_refine;
