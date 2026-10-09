-- A column with statistics but without a `uniq` sketch has no distinct count. Without this the
-- estimator exported a tenth of the rows as the column's NDV and the join took it for a measured
-- value: `t_nu_fact.k` has ten values over 1000 rows, and the join estimated 100 rows against 1000.
SET allow_experimental_statistics = 1;
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

DROP TABLE IF EXISTS t_nu_fact;
DROP TABLE IF EXISTS t_nu_dim;

-- Sparse serialization changes the bytes a read costs; the runner randomizes it.
CREATE TABLE t_nu_fact (k UInt64 STATISTICS(basic), v UInt64)
    ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_nu_dim (k UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_nu_fact SELECT number % 10, number FROM numbers(1000);
INSERT INTO t_nu_dim SELECT number FROM numbers(10);

SELECT '-- every fact row meets one dimension row: 1000 rows';
EXPLAIN estimates = 1 SELECT count() FROM t_nu_fact AS f JOIN t_nu_dim AS d ON f.k = d.k;
SELECT count() FROM t_nu_fact AS f JOIN t_nu_dim AS d ON f.k = d.k;

DROP TABLE t_nu_fact;
DROP TABLE t_nu_dim;
