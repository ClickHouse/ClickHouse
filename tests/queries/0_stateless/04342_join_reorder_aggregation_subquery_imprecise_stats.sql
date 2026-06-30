-- The imprecise estimate makes join reordering log a diagnostic message; suppress server logs from
-- the client log stream so the test harness does not treat it as unexpected stderr output.
SET send_logs_level = 'fatal';

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET allow_experimental_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t_agg_no_stats;
DROP TABLE IF EXISTS t_agg_stats;

-- `auto_statistics_types` defaults to a non-empty value, so MergeTree tables get column statistics
-- automatically. Disable it to model the case where no statistics are available.
CREATE TABLE t_agg_no_stats (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_agg_stats (id UInt64 STATISTICS(uniq), w UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_agg_no_stats SELECT number, number FROM numbers(1000);
INSERT INTO t_agg_stats SELECT number, number FROM numbers(100);

OPTIMIZE TABLE t_agg_no_stats FINAL;
OPTIMIZE TABLE t_agg_stats FINAL;

SET use_statistics = 1;

-- The aggregation subquery reads from a table without statistics. Its row estimate is derived from the
-- MergeTree fallback, so the imprecise/`no_stats~` annotation must be carried through the aggregation
-- (estimateAggregatingStepStats) and reach join reordering, instead of being silently reset to precise.
SELECT '-- aggregation subquery over a table without statistics keeps the no_stats label --';
SELECT trimLeft(explain) FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM
        (SELECT k, count() AS c FROM t_agg_no_stats GROUP BY k) AS agg
        JOIN t_agg_stats AS s ON agg.k = s.id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_agg_no_stats;
DROP TABLE t_agg_stats;
