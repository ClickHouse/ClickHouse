-- Tags: no-random-settings

-- Test: LIMIT push-down into aggregation-in-order
-- When GROUP BY key = ORDER BY key = table PK and LIMIT is present,
-- the aggregation should stop early after producing enough groups.

DROP TABLE IF EXISTS t_agg_in_order_limit;

CREATE TABLE t_agg_in_order_limit (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 16;

-- Insert 1000 rows: 100 distinct keys (0..99), 10 rows each.
-- Use ORDER BY to ensure data is inserted in sorted order within single part.
INSERT INTO t_agg_in_order_limit SELECT number % 100 AS key, number AS value FROM numbers(1000) ORDER BY key;

-- Basic: GROUP BY pk ORDER BY pk LIMIT
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- With OFFSET
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 3 OFFSET 5
SETTINGS optimize_aggregation_in_order = 1;

-- Multiple aggregate functions
SELECT key, count(), sum(value) FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: HAVING present — should still return correct results
SELECT key, count() AS c FROM t_agg_in_order_limit GROUP BY key HAVING c > 5 ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: ORDER BY DESC — should still return correct results
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key DESC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: WITH TOTALS — should still return correct results
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key WITH TOTALS ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: ARRAY JOIN between aggregation and outer LIMIT changes row count;
-- the optimization must not push the limit through ExpressionStep that has ARRAY JOIN.
SELECT key, count() FROM (
    SELECT key FROM t_agg_in_order_limit GROUP BY key
) ARRAY JOIN [1, 2, 3] AS x
GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: extremes are computed before LIMIT — pushing the limit past
-- ExtremesStep would give wrong min/max.
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, extremes = 1;

-- Regression: an ExpressionStep rewrites the ORDER BY key after aggregation
-- but reuses the same user-visible name (`-key AS k`). The optimization must
-- not push LIMIT into the aggregator based on a name match alone — the sort
-- column refers to the post-expression value while the aggregator emits the
-- pre-expression key. Result must match between the two settings.
SELECT -key AS k, count() FROM t_agg_in_order_limit GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1;

SELECT -key AS k, count() FROM t_agg_in_order_limit GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0;

-- Regression: in the basic positive case (GROUP BY pk ORDER BY pk LIMIT) the
-- LIMIT push-down must actually reach the aggregator and short-circuit the read.
-- The small-block settings expose the effect on this 1000-row table — without
-- push-down all rows are read, with push-down only a few granules.
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_threads = 1, max_block_size = 16,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_rows_for_seek = 0,
         log_comment = '04234_positive_on';

SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0,
         max_threads = 1, max_block_size = 16,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_rows_for_seek = 0,
         log_comment = '04234_positive_off';

SYSTEM FLUSH LOGS query_log;

SELECT if(on_reads < off_reads, 'PUSHDOWN_FIRES', format('FAIL: on={} off={}', on_reads, off_reads))
FROM (
    SELECT
        anyIf(read_rows, log_comment = '04234_positive_on') AS on_reads,
        anyIf(read_rows, log_comment = '04234_positive_off') AS off_reads
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('04234_positive_on', '04234_positive_off')
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
      AND event_time >= now() - 600
);

DROP TABLE t_agg_in_order_limit;
