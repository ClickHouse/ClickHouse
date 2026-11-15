-- Tags: no-parallel-replicas, no-object-storage
-- The sliced pool is used for local reading only; the bounds below assume the slice sizes of parts
-- on local disks.

-- The read-ahead budget of the sliced pool follows the evidence: four times the marks of the slices the
-- merge took in full (the rows may be all the query needs) plus sixteen times the marks of the slices
-- that came back without any rows (the query is scanning for rows far away or not there), up to a full
-- slice per source.
-- Here a one-row target just past the ramp, with two rows per granule passing the reader, is found
-- with a few full slices in flight instead of a full slice per source issued at once, which would be
-- the whole part; a target deep in the part, with nothing passing the reader, is reached with every
-- source reading.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET read_in_order_two_level_merge_threshold = 100;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET materialize_statistics_on_insert = 0;
SET max_block_size = 1024;
SET max_insert_threads = 1;
-- Slices of at most 64 marks of 128 rows: the ramp of a lane is 1, 2, 4, 8, 16 and 32 marks, 63 in
-- total, and the budget is capped at 16 sources times 64 marks, more than the part has.
SET merge_tree_min_rows_for_concurrent_read = 8192;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET max_threads = 16;

DROP TABLE IF EXISTS t_sliced_depth;

-- One part of 65536 rows (512 granules): key 8256 lies in the 65th granule, the first past the ramp;
-- key 60000 lies 469 granules in; every 64th key carries v = 3, two rows per granule.
CREATE TABLE t_sliced_depth (x UInt64, v UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_sliced_depth;
INSERT INTO t_sliced_depth SELECT number, multiIf(number = 8256, 1, number = 60000, 2, number % 64 = 0, 3, 0) FROM numbers(65536);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_depth' AND active;

-- The reader lets the v = 3 rows through and the WHERE drops them after the merge, so every slice comes
-- back with a few rows: the slow speed. Five times: how far the read-ahead gets before the limit cancels
-- it depends on timing (see below).
SELECT 'one row just past the ramp', x FROM t_sliced_depth PREWHERE v IN (1, 3) WHERE v = 1 ORDER BY x LIMIT 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05317_near';
SELECT 'one row just past the ramp', x FROM t_sliced_depth PREWHERE v IN (1, 3) WHERE v = 1 ORDER BY x LIMIT 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05317_near';
SELECT 'one row just past the ramp', x FROM t_sliced_depth PREWHERE v IN (1, 3) WHERE v = 1 ORDER BY x LIMIT 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05317_near';
SELECT 'one row just past the ramp', x FROM t_sliced_depth PREWHERE v IN (1, 3) WHERE v = 1 ORDER BY x LIMIT 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05317_near';
SELECT 'one row just past the ramp', x FROM t_sliced_depth PREWHERE v IN (1, 3) WHERE v = 1 ORDER BY x LIMIT 1 SETTINGS optimize_move_to_prewhere = 0, log_comment = '05317_near';
SELECT 'one row just past the ramp', x FROM t_sliced_depth PREWHERE v IN (1, 3) WHERE v = 1 ORDER BY x LIMIT 1 SETTINGS optimize_move_to_prewhere = 0, read_in_order_use_sliced_pool = 0;
-- Nothing passes the reader before the row: the fast speed, every source reading.
SELECT 'one row deep in the part', x FROM t_sliced_depth WHERE v = 2 ORDER BY x LIMIT 1 SETTINGS log_comment = '05317_deep';
SELECT 'one row deep in the part', x FROM t_sliced_depth WHERE v = 2 ORDER BY x LIMIT 1 SETTINGS read_in_order_use_sliced_pool = 0;

SYSTEM FLUSH LOGS query_log;

-- Rows read:
--   near: the ramp (63 marks), then the budget of four times the consumed marks lets three full slices
--     in flight, the first of which holds the row: 255 marks. Slices are consumed as the merge takes
--     their rows and grow the budget further, so what is read beyond that before the limit cancels the
--     sources is a race with the merge: from 10000 rows up to most of the part. The least of five runs
--     stays under half the part and pins the slow speed, as a full slice per source issued at once
--     reads the whole part every time;
--   deep: the 469 granules up to the row at least, and at most the part (65536), whatever the timing.
SELECT 'near', min(read_rows) <= 32768 AS within_bound, max(result_rows) AS result_rows
FROM (SELECT read_rows, result_rows FROM system.query_log
      WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05317_near'
      ORDER BY event_time_microseconds DESC LIMIT 5);
SELECT 'deep', read_rows BETWEEN 60032 AND 65536 AS within_bound, result_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05317_deep'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_sliced_depth;
