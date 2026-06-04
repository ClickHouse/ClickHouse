-- Regression test: distributed aggregation must not use the no-merge shuffle strategy where it
-- would produce wrong results. The distributed result must match the single-node result.
--   * GROUPING SETS: shuffle scatters by the full key set, so subtotal rows (grouped by key
--     subsets) would be produced independently in several buckets and duplicated.
--   * A global GROUP BY limit (max_rows_to_group_by): per-bucket aggregation cannot honor it.
-- Results are wrapped in an order-independent aggregate so the check is robust to row order but
-- still detects duplicated subtotals.

DROP TABLE IF EXISTS t_agg_guard;

CREATE TABLE t_agg_guard (a UInt32, b UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO t_agg_guard SELECT number % 10, number % 7, number FROM numbers(100000);

-- distributed_plan_max_rows_to_broadcast is kept high so the read stays single-stage; the test
-- targets the aggregation strategy, which distributed_plan_force_shuffle_aggregation would push to
-- the (incorrect) no-merge Shuffle path were the guards not in place.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 1000000000, enable_join_runtime_filters = 0,
    distributed_plan_force_shuffle_aggregation = 1;

SELECT '-- GROUPING SETS (distributed)';
SELECT count(), sum(a), sum(b), sum(s)
FROM (SELECT a, b, sum(v) AS s FROM t_agg_guard GROUP BY GROUPING SETS ((a), (b), ()));

SELECT '-- GROUPING SETS (baseline, must match)';
SELECT count(), sum(a), sum(b), sum(s)
FROM (SELECT a, b, sum(v) AS s FROM t_agg_guard GROUP BY GROUPING SETS ((a), (b), ()))
SETTINGS make_distributed_plan = 0;

SELECT '-- max_rows_to_group_by: a global limit must still throw, not be diluted across buckets';
SELECT a, sum(v) FROM t_agg_guard GROUP BY a
SETTINGS max_rows_to_group_by = 5, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_agg_guard;
