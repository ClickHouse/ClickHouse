-- A join records the rows it built its hash table from, and the next plan reuses that count as an
-- estimate of the build input. Here `t_rf_b` is the build side of the inner join, and a runtime filter
-- from the outer join (`b.k2 = c.k2`) drops 95% of its rows. That count depends on the outer join, so
-- the next plan must not take it as the size of `t_rf_b`. Otherwise the inner join is estimated at 50
-- rows, not 1000, and the plans of repeated runs differ. The check reads the estimate of the inner join only:
-- the rest of the plan varies with settings the runner randomizes.
SET allow_experimental_statistics = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET collect_hash_table_stats_during_joins = 1;
SET use_hash_table_stats_for_join_reordering = 1;

DROP TABLE IF EXISTS t_rf_a;
DROP TABLE IF EXISTS t_rf_b;
DROP TABLE IF EXISTS t_rf_c;

CREATE TABLE t_rf_a (k1 UInt64 STATISTICS(uniq, basic)) ENGINE = MergeTree ORDER BY k1 SETTINGS auto_statistics_types = '';
CREATE TABLE t_rf_b (k1 UInt64 STATISTICS(uniq, basic), k2 UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k1 SETTINGS auto_statistics_types = '';
CREATE TABLE t_rf_c (k2 UInt64 STATISTICS(uniq, basic)) ENGINE = MergeTree ORDER BY k2 SETTINGS auto_statistics_types = '';
INSERT INTO t_rf_a SELECT number FROM numbers(10000);
INSERT INTO t_rf_b SELECT number, number % 100 FROM numbers(1000);
INSERT INTO t_rf_c SELECT number % 5 FROM numbers(500);

SELECT count() FROM t_rf_a AS a JOIN t_rf_b AS b ON a.k1 = b.k1 JOIN t_rf_c AS c ON b.k2 = c.k2;

SELECT '-- the inner join estimate after one run';
SELECT regexpExtract((groupArrayIf(explain, explain LIKE '%Join (%') AS joins)[length(joins)], '\\(rows: [^,)]*')
FROM (EXPLAIN estimates = 1 SELECT count() FROM t_rf_a AS a JOIN t_rf_b AS b ON a.k1 = b.k1 JOIN t_rf_c AS c ON b.k2 = c.k2);

SELECT count() FROM t_rf_a AS a JOIN t_rf_b AS b ON a.k1 = b.k1 JOIN t_rf_c AS c ON b.k2 = c.k2;

SELECT '-- the inner join estimate after two runs';
SELECT regexpExtract((groupArrayIf(explain, explain LIKE '%Join (%') AS joins)[length(joins)], '\\(rows: [^,)]*')
FROM (EXPLAIN estimates = 1 SELECT count() FROM t_rf_a AS a JOIN t_rf_b AS b ON a.k1 = b.k1 JOIN t_rf_c AS c ON b.k2 = c.k2);

DROP TABLE t_rf_a;
DROP TABLE t_rf_b;
DROP TABLE t_rf_c;
