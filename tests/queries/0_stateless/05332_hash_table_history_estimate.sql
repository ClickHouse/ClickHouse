-- The row count a past execution built a join's hash table from replaces a larger estimate of that
-- input in the next plan and stays an imprecise estimate: it describes the past execution's input,
-- not this one's. `EXPLAIN` shows the source.
SET allow_experimental_statistics = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 1;
SET use_hash_table_stats_for_join_reordering = 1;

DROP TABLE IF EXISTS t_hist_probe;
DROP TABLE IF EXISTS t_hist_build;

CREATE TABLE t_hist_probe (k UInt64 STATISTICS(uniq, basic), v UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_hist_build (k UInt64 STATISTICS(uniq, basic), v UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_hist_probe SELECT number, number FROM numbers(10000);
INSERT INTO t_hist_build SELECT number * 100, number FROM numbers(100);

-- `k` and `v` are correlated (`k = v * 100`), so the statistics overestimate the two conditions
-- as independent (40 rows) and the 30 rows measured by the first execution replace them in the
-- second plan.
SELECT count() FROM t_hist_probe AS p JOIN t_hist_build AS b ON p.k = b.k WHERE b.v < 50 AND b.k >= 2000;

SELECT '-- the measured build rows replace the statistics estimate and stay imprecise';
EXPLAIN estimates = 1 SELECT count() FROM t_hist_probe AS p JOIN t_hist_build AS b ON p.k = b.k WHERE b.v < 50 AND b.k >= 2000;
SELECT countIf(explain LIKE '%ResultRows: ~~%') > 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1 SELECT count() FROM t_hist_probe AS p JOIN t_hist_build AS b ON p.k = b.k WHERE b.v < 50 AND b.k >= 2000
);

DROP TABLE t_hist_probe;
DROP TABLE t_hist_build;
