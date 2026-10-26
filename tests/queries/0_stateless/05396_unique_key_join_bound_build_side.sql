-- The only key of an aggregation is unique, so a join on it keeps at most the rows of the other side: here the
-- filtered `t_ub_dim` has no estimate, only a bound of 1000 rows, and its join with the aggregation keeps that bound.
-- The bound is below the known 100000 rows of `t_ub_fact`, so the hash table is built from the join of the two and
-- `t_ub_fact` is the probe side. Without the bound the table with the known size would be built.
-- The query runs before the `EXPLAIN`, so the sizes of its hash tables would give the inputs estimates.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 0;

DROP TABLE IF EXISTS t_ub_fact;
DROP TABLE IF EXISTS t_ub_dim;
CREATE TABLE t_ub_fact (k UInt64, q UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_ub_dim (k UInt64, c String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_ub_fact SELECT number % 1000, number % 7 FROM numbers(100000);
INSERT INTO t_ub_dim SELECT number, toString(number % 10) FROM numbers(1000);

SELECT count() FROM t_ub_dim AS d, (SELECT k, avg(q) AS m FROM t_ub_fact GROUP BY k) AS a, t_ub_fact AS f
WHERE d.c = '3' AND a.k = d.k AND f.k = d.k AND f.q < a.m;

SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_limit = 10;
SELECT replaceRegexpAll(replaceAll(explain, currentDatabase() || '.', ''), ', cost: [0-9.e+]+|, cost: <unknown>', '')
FROM (
    EXPLAIN estimates = 1, bounds = 1
    SELECT count() FROM t_ub_dim AS d, (SELECT k, avg(q) AS m FROM t_ub_fact GROUP BY k) AS a, t_ub_fact AS f
    WHERE d.c = '3' AND a.k = d.k AND f.k = d.k AND f.q < a.m
    SETTINGS use_hash_table_stats_for_join_reordering = 0
)
WHERE explain LIKE '%Join (%' OR explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Aggregating (%';

DROP TABLE t_ub_fact;
DROP TABLE t_ub_dim;
