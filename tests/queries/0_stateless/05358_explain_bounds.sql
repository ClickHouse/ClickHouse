-- `EXPLAIN estimates = 1, bounds = 1` adds to a row estimate the proven upper bound, and says when the
-- estimate rests on a default selectivity. Here `name LIKE '%7%'` has no statistics, so the join size is
-- taken from defaults, and the bound is the product of the two inputs. Only the estimate of the join is checked:
-- the rest of the plan varies with settings the runner randomizes.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET allow_experimental_statistics = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_bounds_fact;
DROP TABLE IF EXISTS t_bounds_dim;

CREATE TABLE t_bounds_fact (k UInt64 STATISTICS(uniq, basic), v String)
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_bounds_dim (k UInt64 STATISTICS(uniq, basic), name String)
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_bounds_fact SELECT number % 100, toString(number) FROM numbers(10000);
INSERT INTO t_bounds_dim SELECT number, toString(number) FROM numbers(100);

SELECT replaceRegexpAll(regexpExtract(explain, '\\(rows: .*\\)$'), ', cost: [0-9.e+]+', '')
FROM (
    EXPLAIN estimates = 1, bounds = 1
    SELECT count() FROM t_bounds_fact AS f JOIN t_bounds_dim AS d ON f.k = d.k WHERE d.name LIKE '%7%'
)
WHERE explain LIKE '%Join (%';

SELECT '-- without bounds';
SELECT replaceRegexpAll(regexpExtract(explain, '\\(rows: .*\\)$'), ', cost: [0-9.e+]+', '')
FROM (
    EXPLAIN estimates = 1
    SELECT count() FROM t_bounds_fact AS f JOIN t_bounds_dim AS d ON f.k = d.k WHERE d.name LIKE '%7%'
)
WHERE explain LIKE '%Join (%';

DROP TABLE t_bounds_fact;
DROP TABLE t_bounds_dim;
