-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- Integration of the filter selectivity estimate into the Cascades memo. The per-predicate
-- formulas are covered by `gtest_cascades_filter_selectivity`; here `EXPLAIN estimates = 1`
-- shows the memo row estimates. The outer probe queries turn distributed planning off
-- because they read the plan through `viewExplain`, which Cascades rejects.

DROP TABLE IF EXISTS t_filter_sel;
DROP TABLE IF EXISTS t_filter_dim;
CREATE TABLE t_filter_sel (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_filter_dim (k UInt32) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
SYSTEM STOP MERGES t_filter_sel;
SYSTEM STOP MERGES t_filter_dim;
INSERT INTO t_filter_sel SELECT number, number FROM numbers(10000);
INSERT INTO t_filter_dim SELECT number FROM numbers(1000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
-- Pinned (randomized in CI): join-order jitter changes the plan shape around the asserted filters.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'dpsize greedy';
SET allow_experimental_correlated_subqueries = 1;
SET param__internal_cascades_cluster_node_count = 8;
SET param__internal_join_table_stat_hints = '{"t_filter_sel": {"cardinality": 600000000, "avg_row_bytes": 12, "distinct_keys": {"k": 6000000, "v": 100}}, "t_filter_dim": {"cardinality": 3000000, "avg_row_bytes": 12, "distinct_keys": {"k": 3000000}}}';

-- `HAVING` compares an aggregate result, so the default factor 0.33 applies: the 6000000-row
-- aggregation estimate becomes 1980000. Before, the filter kept the full input estimate.
SELECT 'default factor:', countIf(explain LIKE '%Filter (HAVING) (rows: ~1980000%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT sum(v) AS s FROM t_filter_sel GROUP BY k HAVING s > 300
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- The decorrelated `EXISTS` leaves a filter that repeats the join equality (removes nothing:
-- the keys are one equivalence class) next to the correlated `<>` (removes 1/NDV): the
-- 300000000-row join estimate keeps 300000000 x (1 - 1/3000000) = 299999900 rows.
SELECT 'join equality and <>:', countIf(explain LIKE '%Filter%(rows: ~299999900.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT count() FROM t_filter_sel AS a
    WHERE EXISTS (SELECT 1 FROM t_filter_dim AS b WHERE b.k = a.k AND b.k <> a.v)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1,
        allow_experimental_correlated_subqueries = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_filter_dim;
DROP TABLE t_filter_sel;
