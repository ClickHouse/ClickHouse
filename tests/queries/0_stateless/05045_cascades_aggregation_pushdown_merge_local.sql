-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- The `cascades_aggregation_pushdown` variant-A top step is a merge-only `Aggregating`
-- (`Params::only_merge`); under the default cost weights it takes the Local strategy: gather the
-- state rows and merge on one node. This file pins that shape via EXPLAIN conjuncts and executes
-- it with a result check against the classic plan.
--
-- Every executed pushed query here inherently exercises worker re-optimization of the serialized
-- fragment: Cascades requires `make_distributed_plan`, and locally-executed fragments always
-- round-trip through `serializeQueryPlan`/`deserializeQueryPlan`, after which the worker reruns
-- the classic optimizer passes (`QueryPlan::optimize`) over a plan containing the merge-only
-- `Aggregating` - covering the optimizer consumers audited for `only_merge`, including the
-- defensive bail-out in `optimizeUseAggregateProjections`.

DROP TABLE IF EXISTS t_ml_facts;
DROP TABLE IF EXISTS t_ml_dims;

CREATE TABLE t_ml_facts (key UInt32, value Int64) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_ml_dims (key UInt32, name String) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_ml_facts;
SYSTEM STOP MERGES t_ml_dims;

INSERT INTO t_ml_facts SELECT number % 10, number FROM numbers(1000);
INSERT INTO t_ml_dims SELECT number, concat('name_', toString(number)) FROM numbers(8);

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
-- The Cascades cost model's parallelism input follows `max_threads`; pin it so the chosen merge
-- strategy does not depend on the machine's core count.
SET max_threads = 32;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_ml_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_ml_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

-- Local-merge evidence, pinned as the full legacy EXPLAIN: the variant-A sandwich (the merge-only
-- `Aggregating` above the `JoinLogical`, the partial below it) with a `GatherExchange` and no
-- `ShuffleExchange` - the Shuffle merge strategy would repartition instead of gathering (see
-- 05046_cascades_aggregation_pushdown_merge_shuffle for that shape).
SELECT '-- canary: variant A with a Local merge (gather, no shuffle)';
EXPLAIN SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_ml_facts AS t1 LEFT JOIN t_ml_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy';

SELECT '-- execution through the Local merge';
SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_ml_facts AS t1 LEFT JOIN t_ml_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;

SELECT '-- the same execution without the distributed planner must match';
SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_ml_facts AS t1 LEFT JOIN t_ml_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_ml_facts;
DROP TABLE t_ml_dims;
