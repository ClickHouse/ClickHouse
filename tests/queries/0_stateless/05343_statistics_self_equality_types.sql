-- `x = x` is true on every row only when the value type makes it so. The statistics estimator
-- takes it as always true for a non-nullable, non-float column; for a Nullable column `equals`
-- gives NULL on NULL rows and for a floating point column NaN is not equal to itself, so those
-- stay unknown predicates with the default selectivity. `isNotDistinctFrom(x, x)` is true for a
-- Nullable column as well.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS t_se;
DROP TABLE IF EXISTS t_se_other;
CREATE TABLE t_se (k UInt64, i UInt64 STATISTICS(uniq, basic), n Nullable(UInt64) STATISTICS(uniq, basic), f Float64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_se_other (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_se SELECT number, number, if(number % 2 = 0, NULL, number), number FROM numbers(1000);
INSERT INTO t_se_other SELECT number FROM numbers(1000);

SELECT '-- non-nullable integer: all 1000 rows';
EXPLAIN estimates = 1 SELECT count() FROM t_se AS a JOIN t_se_other AS o ON a.k = o.k WHERE a.i = a.i;
SELECT '-- Nullable equals: an unknown predicate, the default selectivity';
EXPLAIN estimates = 1 SELECT count() FROM t_se AS a JOIN t_se_other AS o ON a.k = o.k WHERE a.n = a.n;
SELECT '-- Nullable isNotDistinctFrom: all rows';
EXPLAIN estimates = 1 SELECT count() FROM t_se AS a JOIN t_se_other AS o ON a.k = o.k WHERE isNotDistinctFrom(a.n, a.n);
SELECT '-- Float64: an unknown predicate';
EXPLAIN estimates = 1 SELECT count() FROM t_se AS a JOIN t_se_other AS o ON a.k = o.k WHERE a.f = a.f;

DROP TABLE t_se;
DROP TABLE t_se_other;
