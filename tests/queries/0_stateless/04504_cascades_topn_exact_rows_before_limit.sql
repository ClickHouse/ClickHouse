-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- With `exact_rows_before_limit` the per-shard sorts of a two-stage top-N must feed the full
-- row count into `rows_before_limit_at_least`, but the internal per-shard cap cuts the pipeline
-- walk that collects those counters. The optimizer must not build the two-stage top-N then.

DROP TABLE IF EXISTS t_topn_exact;
CREATE TABLE t_topn_exact (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_topn_exact;
INSERT INTO t_topn_exact SELECT number % 1000, number FROM numbers(10000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_topn_exact": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"k": 1000}}}';

SELECT '-- 1. default: two-stage top-N with a per-shard cap';
EXPLAIN SELECT v FROM t_topn_exact ORDER BY v LIMIT 3;

SELECT '-- 2. exact_rows_before_limit: one full sort instead';
EXPLAIN SELECT v FROM t_topn_exact ORDER BY v LIMIT 3 SETTINGS exact_rows_before_limit = 1;

SELECT '-- 3. same results in both modes';
SELECT v FROM t_topn_exact ORDER BY v LIMIT 3;
SELECT v FROM t_topn_exact ORDER BY v LIMIT 3 SETTINGS exact_rows_before_limit = 1;

DROP TABLE t_topn_exact;
