-- Tags: no-parallel-replicas
-- ^ because we are using query_log
-- Test read_in_order_use_virtual_row optimization for DESC (reverse) order
-- with multiple parts. PR #99198 extends the virtual row path to InReverseOrder reads.

SET use_query_condition_cache = 0;
-- Pin query_plan_read_in_order because the optimization requires BOTH
-- optimize_read_in_order AND query_plan_read_in_order to be enabled
-- (see QueryPlanOptimizationSettings.cpp). CI may randomize the latter to 0.

DROP TABLE IF EXISTS t_04045_rvrow;

-- index_granularity_bytes = 10485760: disables adaptive granularity so
-- index_granularity = 8192 is the effective granule size regardless of the
-- random MergeTree settings the flaky check injects (e.g. index_granularity_bytes = 1588).
-- add_minmax_index_for_numeric_columns = 0: prevents automatic minmax indexes
-- from changing read_rows (same guard used in 03031).
-- Default max_threads is intentional: single-threaded execution can mask
-- virtual-row bugs by always reading parts in a predictable order.
CREATE TABLE t_04045_rvrow (x UInt64, y UInt64)
ENGINE = MergeTree ORDER BY (x, y)
SETTINGS index_granularity = 8192,
         index_granularity_bytes = 10485760,
         add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_04045_rvrow;

-- Two non-overlapping parts (50 granules each): rows 0..409599 and 409600..819199.
-- Full table = 8192 * 100 = 819200 rows; optimized path reads << 1/10 of that.
INSERT INTO t_04045_rvrow SELECT number, number FROM numbers(8192 * 50);
INSERT INTO t_04045_rvrow SELECT number + 8192 * 50, number + 8192 * 50 FROM numbers(8192 * 50);

-- Run all queries first, then single SYSTEM FLUSH LOGS, then batch assertions.

-- DESC with preliminary merge (two_level_merge_threshold = 0)
SELECT x FROM t_04045_rvrow
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         optimize_read_in_order = 1, query_plan_read_in_order = 1, max_block_size = 8192,
         log_comment = 'desc_prelim_merge';

-- DESC without preliminary merge (threshold above part count)
SELECT x FROM t_04045_rvrow
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 5,
         optimize_read_in_order = 1, query_plan_read_in_order = 1, max_block_size = 8192,
         log_comment = 'desc_no_prelim_merge';

-- DESC with filter: PK prunes part 2 entirely (all rows >= 409600 fail x < 16384)
SELECT x FROM t_04045_rvrow
WHERE x < 8192 * 2
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         optimize_read_in_order = 1, query_plan_read_in_order = 1, max_block_size = 8192,
         log_comment = 'desc_filter';

-- DESC multi-column key: ORDER BY (x DESC, y DESC) matches table key reversed
SELECT x, y FROM t_04045_rvrow
ORDER BY x DESC, y DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         optimize_read_in_order = 1, query_plan_read_in_order = 1, max_block_size = 8192,
         log_comment = 'desc_multicol';

SYSTEM FLUSH LOGS query_log;

-- Virtual rows allow skipping all but the last few granules per part.
-- Threshold 8192 * 10 (10 granules) is << 819200 total rows.
SELECT if(read_rows < 8192 * 10, 'OK', format('Fail desc_prelim_merge: read_rows={}', read_rows))
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'desc_prelim_merge'
    AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

SELECT if(read_rows < 8192 * 10, 'OK', format('Fail desc_no_prelim_merge: read_rows={}', read_rows))
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'desc_no_prelim_merge'
    AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

-- Filter case: PK prunes part 2; at most 2 qualifying granules in part 1.
SELECT if(read_rows < 8192 * 5, 'OK', format('Fail desc_filter: read_rows={}', read_rows))
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'desc_filter'
    AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

SELECT if(read_rows < 8192 * 10, 'OK', format('Fail desc_multicol: read_rows={}', read_rows))
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'desc_multicol'
    AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

DROP TABLE t_04045_rvrow;
