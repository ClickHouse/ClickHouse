-- Tags: no-random-settings, no-parallel-replicas

-- Test: multithreaded LIMIT push-down into aggregation-in-order.
--
-- The single-thread path (04234) early-finishes a single
-- `AggregatingInOrderTransform`. With `max_threads` > 1 and several parts the
-- pipeline instead fans out into multiple `AggregatingInOrderTransform`s feeding
-- one `FinishAggregatingInOrderTransform`; the limit must short-circuit the
-- `FinishAggregatingInOrderAlgorithm` merge after it has produced enough groups.

DROP TABLE IF EXISTS t_agg_in_order_limit_mt;

CREATE TABLE t_agg_in_order_limit_mt (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 16;

-- Keep the parts separate so several ordered streams reach the merge.
SYSTEM STOP MERGES t_agg_in_order_limit_mt;

-- 4 parts, each holding keys 0..99 (10 rows per key). Every part spans the full
-- key range, so the finish-aggregating merge has to combine each group across
-- all four streams before it can emit it.
INSERT INTO t_agg_in_order_limit_mt SELECT number % 100, number FROM numbers(1000);
INSERT INTO t_agg_in_order_limit_mt SELECT number % 100, number FROM numbers(1000);
INSERT INTO t_agg_in_order_limit_mt SELECT number % 100, number FROM numbers(1000);
INSERT INTO t_agg_in_order_limit_mt SELECT number % 100, number FROM numbers(1000);

-- The multi-stream pipeline is selected: `FinishAggregatingInOrder` only exists
-- when more than one `AggregatingInOrderTransform` runs in parallel.
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE
    SELECT key, count() FROM t_agg_in_order_limit_mt GROUP BY key ORDER BY key ASC LIMIT 5
    SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1
) WHERE explain LIKE '%FinishAggregatingInOrder%';

-- Correctness: result with push-down on must match the expected groups.
SELECT key, count(), sum(value) FROM t_agg_in_order_limit_mt GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1;

-- With OFFSET.
SELECT key, count() FROM t_agg_in_order_limit_mt GROUP BY key ORDER BY key ASC LIMIT 3 OFFSET 5
SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1;

-- Self-consistency: push-down on vs off must produce byte-identical results.
SELECT 'consistent' FROM (
    SELECT groupArray((key, c, s)) AS res_on FROM (
        SELECT key, count() AS c, sum(value) AS s FROM t_agg_in_order_limit_mt
        GROUP BY key ORDER BY key ASC LIMIT 17
        SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1
    )
) AS a
INNER JOIN (
    SELECT groupArray((key, c, s)) AS res_off FROM (
        SELECT key, count() AS c, sum(value) AS s FROM t_agg_in_order_limit_mt
        GROUP BY key ORDER BY key ASC LIMIT 17
        SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0
    )
) AS b ON 1 = 1
WHERE res_on = res_off;

-- Read short-circuiting: with the limit pushed down the multi-stream pipeline
-- must read fewer rows than without it. Small blocks expose the early stop on
-- this 4000-row table.
SELECT key, count() FROM t_agg_in_order_limit_mt GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_block_size = 16,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_rows_for_seek = 0,
         log_comment = '04327_mt_on';

SELECT key, count() FROM t_agg_in_order_limit_mt GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0,
         max_block_size = 16,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_rows_for_seek = 0,
         log_comment = '04327_mt_off';

SYSTEM FLUSH LOGS query_log;

SELECT if(on_reads < off_reads, 'PUSHDOWN_FIRES', format('FAIL: on={} off={}', on_reads, off_reads))
FROM (
    SELECT
        anyIf(read_rows, log_comment = '04327_mt_on') AS on_reads,
        anyIf(read_rows, log_comment = '04327_mt_off') AS off_reads
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('04327_mt_on', '04327_mt_off')
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
      AND event_time >= now() - 600
);

DROP TABLE t_agg_in_order_limit_mt;
