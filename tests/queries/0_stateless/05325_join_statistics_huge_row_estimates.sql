-- Join planning with row estimates beyond the 64-bit integer range: the hints make `t1, t2`
-- a 1e22-row cross product. Every query must plan and return its result.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';
CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';
CREATE TABLE t3 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';
INSERT INTO t1 VALUES (1, 1);
INSERT INTO t2 VALUES (1, 1);
INSERT INTO t3 VALUES (1), (2);

SET enable_analyzer = 1;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET use_index_for_in_with_subqueries = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{
    "t1": { "cardinality": 100000000000, "distinct_keys": { "a": 100000000000 } },
    "t2": { "cardinality": 100000000000, "distinct_keys": { "a": 100000000000 } },
    "t3": { "cardinality": 10, "distinct_keys": { "a": 10 } }
}';

-- The huge side is the left input of a semi join.
SELECT count() FROM t1, t2 WHERE t1.a IN (SELECT a FROM t3) SETTINGS rewrite_in_to_join = 1, query_plan_join_swap_table = 1;
-- The huge side is the right input of a semi join.
SELECT count() FROM t3 LEFT SEMI JOIN (SELECT t1.a AS a FROM t1, t2) AS s ON t3.a = s.a;
-- Aggregation, DISTINCT and INTERSECT over the huge side, keyed on a column without statistics.
SELECT t1.b + t2.b AS k, count() FROM t1, t2 GROUP BY k;
SELECT DISTINCT t1.b, t1.b + t2.b FROM t1, t2;
SELECT t1.b, t2.b FROM t1, t2 INTERSECT DISTINCT SELECT a, a FROM t3;
-- The same overflow in the join order optimizer, without Cascades.
SELECT count() FROM (SELECT number % 3 AS k, count() AS c FROM (SELECT number FROM numbers(10) LIMIT 18446744073709551615) GROUP BY k) AS agg INNER JOIN t3 ON agg.k = t3.a SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
