-- The shared relation estimator takes the optimized join inside a subquery as the estimate of that
-- subquery, adds up the inputs of `UNION ALL` (the rows, and a column's distinct values as a bound:
-- 100 and 100 values give at most 200, so the join on the union key estimates 11000 rows / 200
-- values per key) and counts the distinct values of a `DISTINCT`. Without this a subquery with a
-- join, a union or a distinct is a relation without an estimate in the query around it. The hints
-- stand in for column statistics.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 3;
SET param__internal_join_table_stat_hints = '{"t_a": {"cardinality": 10000, "distinct_keys": {"k": 100, "v": 50}}, "t_b": {"cardinality": 1000, "distinct_keys": {"k": 100}}, "t_c": {"cardinality": 100, "distinct_keys": {"k": 100}}}';

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;

-- Sparse serialization changes the bytes a read costs; the runner randomizes it.
CREATE TABLE t_a (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_b (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_c (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_a SELECT number % 100, number % 50 FROM numbers(1000);
INSERT INTO t_b SELECT number % 100 FROM numbers(100);
INSERT INTO t_c SELECT number FROM numbers(100);

-- The join inside the subquery is 10000 * 1000 / 100 = 100000 rows and has 100 groups; the outer join
-- of those 100 rows with `t_c` is 100 rows.
SELECT '-- a subquery with a join and an aggregation is a relation with an estimate';
EXPLAIN estimates = 1
SELECT count()
FROM (SELECT a.k AS k, count() AS c FROM t_a AS a JOIN t_b AS b ON a.k = b.k GROUP BY a.k) AS s
JOIN t_c AS c ON s.k = c.k;

-- 10000 + 1000 rows. The union's key has at most 100 + 100 values, so the join is 11000 * 100 / 200 rows.
SELECT '-- UNION ALL adds up its inputs';
EXPLAIN estimates = 1
SELECT count()
FROM (SELECT k FROM t_a UNION ALL SELECT k FROM t_b) AS u
JOIN t_c AS c ON u.k = c.k;

-- 50 distinct values of `v`.
SELECT '-- DISTINCT counts the distinct values of its columns';
EXPLAIN estimates = 1
SELECT count()
FROM (SELECT DISTINCT v FROM t_a) AS d
JOIN t_c AS c ON d.v = c.k;

SELECT '-- Cascades: UNION ALL adds up its inputs';
EXPLAIN estimates = 1
SELECT count()
FROM (SELECT k FROM t_a UNION ALL SELECT k FROM t_b) AS u
JOIN t_c AS c ON u.k = c.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1;

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
