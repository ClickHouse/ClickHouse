-- Tags: no-random-settings

-- LIMIT ... WITH TIES over aggregation in order must keep the whole tie group.

DROP TABLE IF EXISTS t_agg_in_order_limit_with_ties;

CREATE TABLE t_agg_in_order_limit_with_ties (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

INSERT INTO t_agg_in_order_limit_with_ties SELECT number % 10 AS a, number AS b FROM numbers(1000) ORDER BY a, b;

-- The single-stream path is what enforces the limit row-exactly; the multi-stream one can
-- overshoot into the whole tie group on its own, so pin the pipeline before asserting counts.
-- Parallel replicas rewrite the local AggregatingStep into partial aggregation plus merging,
-- so the shape and the row-exact counts only hold with them off.
-- `FinishAggregatingInOrderTransform` contains `AggregatingInOrderTransform` as a substring, so
-- the first conjunct is anchored to reject it.
SELECT countIf(match(explain, '(^|[^A-Za-z])AggregatingInOrderTransform')) > 0
   AND countIf(explain LIKE '%FinishAggregatingInOrderTransform%') = 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
    SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1, max_threads = 1,
             enable_parallel_replicas = 0
);

SELECT count() FROM (
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
) SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1, max_threads = 1,
           enable_parallel_replicas = 0;

SELECT count() FROM (
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
) SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0, max_threads = 1,
           enable_parallel_replicas = 0;

-- The counts above stay 100 whether the push-down pass matched this plan or never ran, so they
-- alone do not establish that the WITH TIES guard was reached. This control is the same
-- `GROUP BY a, b` / `ORDER BY a` prefix shape without WITH TIES, where the push-down is expected
-- to fire, and observes it through `read_rows`. The small-block settings expose the effect on a
-- 1000-row table; `enable_parallel_replicas = 0` is required because `read_rows` is accounted
-- per reading node.
SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 1, max_block_size = 16,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_rows_for_seek = 0,
         enable_parallel_replicas = 0,
         log_comment = '04489_ties_pushdown_on';

SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0,
         max_threads = 1, max_block_size = 16,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_rows_for_seek = 0,
         enable_parallel_replicas = 0,
         log_comment = '04489_ties_pushdown_off';

SYSTEM FLUSH LOGS query_log;

SELECT if(on_reads < off_reads, 'PUSHDOWN_FIRES', format('FAIL: on={} off={}', on_reads, off_reads))
FROM (
    SELECT
        anyIf(read_rows, log_comment = '04489_ties_pushdown_on') AS on_reads,
        anyIf(read_rows, log_comment = '04489_ties_pushdown_off') AS off_reads
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('04489_ties_pushdown_on', '04489_ties_pushdown_off')
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
      AND event_time >= now() - 600
);

DROP TABLE t_agg_in_order_limit_with_ties;
