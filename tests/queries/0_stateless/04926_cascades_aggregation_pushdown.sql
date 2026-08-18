-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- The `cascades_aggregation_pushdown` transformation offers a partial aggregation pushed below
-- a join as a cost-based alternative: `MergingAggregated` above the join, `Aggregating` below
-- on the pushed input. It wins when the pushed side is huge but has few distinct keys.

DROP TABLE IF EXISTS t_push_facts;
DROP TABLE IF EXISTS t_push_dims;
DROP TABLE IF EXISTS t_push_dims_multi;

CREATE TABLE t_push_facts (key UInt32, value Int64) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_push_dims (key UInt32, name String) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_push_dims_multi (key UInt32, threshold Int64) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_push_facts;
SYSTEM STOP MERGES t_push_dims;
SYSTEM STOP MERGES t_push_dims_multi;

INSERT INTO t_push_facts SELECT number % 10, number FROM numbers(1000);
INSERT INTO t_push_dims SELECT number, concat('name_', toString(number)) FROM numbers(8);
-- keys 0, 1, 2 appear 3 times (thresholds 100, 200, 300), keys 3, 4, 5 once (threshold 500),
-- keys 6-9 are absent
INSERT INTO t_push_dims_multi SELECT number % 3, toInt64(100 * (1 + intDiv(number, 3))) FROM numbers(9);
INSERT INTO t_push_dims_multi SELECT number, toInt64(500) FROM numbers(3, 3);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
-- the runtime-filter steps are part of the expected INNER JOIN plan shape, and whether the
-- filter moves into PREWHERE decides between a `Filter` and an `Expression` step in it
SET enable_join_runtime_filters = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

SELECT '-- 1. huge left side, few distinct keys: partial aggregation is pushed below the LEFT JOIN';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 2. same with INNER JOIN';
EXPLAIN SELECT count() FROM t_push_facts AS t1 INNER JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 3. near-unique keys: pushdown does not pay off, classic shape';
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 99000000}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

SELECT '-- 4. disabled by the setting: classic shape';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
SETTINGS cascades_aggregation_pushdown = 0;

SELECT '-- 5a. aggregate with an argument: sum is pushed';
EXPLAIN SELECT sum(t1.value) FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 5b. additional GROUP BY key from the right side: still pushed';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key, t2.name;

SELECT '-- 6. execution: count() per key over LEFT JOIN (keys 0-7 match, 8 and 9 do not)';
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;

SELECT '-- 6b. execution: count() per key over INNER JOIN (keys 8 and 9 drop out)';
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;

SELECT '-- 7. execution: sum(t1.value) per (t1.key, t2.name) over LEFT JOIN';
SELECT t1.key AS k, t2.name AS n, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key, t2.name ORDER BY k, n;

SELECT '-- 8. the same executions without the distributed planner must match';
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
SELECT t1.key AS k, t2.name AS n, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key, t2.name ORDER BY k, n
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100, "value": 1000}}, "t_push_dims_multi": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 100}}}';

SELECT '-- 9. duplicate right-side keys: each pushed group is duplicated by the join and merged m times';
EXPLAIN SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims_multi AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;
SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims_multi AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;

SELECT '-- 10. mixed condition (equi + non-equi): the pushed side groups by (key, value)';
EXPLAIN SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims_multi AS t2 ON t1.key = t2.key AND t1.value > t2.threshold GROUP BY t1.key ORDER BY k;
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims_multi AS t2 ON t1.key = t2.key AND t1.value > t2.threshold GROUP BY t1.key ORDER BY k;

SELECT '-- 11. the same executions without the distributed planner must match';
SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims_multi AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims_multi AS t2 ON t1.key = t2.key AND t1.value > t2.threshold GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_push_facts;
DROP TABLE t_push_dims;
DROP TABLE t_push_dims_multi;
