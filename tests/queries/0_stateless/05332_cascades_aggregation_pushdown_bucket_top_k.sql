-- The cascades `AggregationPushdown` transformation must not be vetoed by the bucket-local Top-K
-- conversion (`Aggregator::Params::bucket_top_k`) of a lone `count`, and must not carry it onto
-- an aggregation it pushes below a join: there it would keep only each bucket's best groups
-- before the join filters them, so the winners that have no match on the right side would push
-- the true answer out.

DROP TABLE IF EXISTS t_abbtk_facts;
DROP TABLE IF EXISTS t_abbtk_dims;

CREATE TABLE t_abbtk_facts (key UInt32) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_abbtk_dims (key UInt32) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_abbtk_facts;
SYSTEM STOP MERGES t_abbtk_dims;

-- 1000 keys with count() = key + 1; only the lower half has a match on the right side, so the
-- best groups of every bucket before the join are all unmatched.
INSERT INTO t_abbtk_facts SELECT number FROM numbers(1000) ARRAY JOIN range(number + 1) AS r;
INSERT INTO t_abbtk_dims SELECT number FROM numbers(500);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET cascades_aggregation_pushdown = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;
-- a runtime join filter would drop the unmatched keys before the aggregation
SET enable_join_runtime_filters = 0;
-- the pre-cascades join-order pass attaches the row estimates the cost model needs (both
-- settings are randomized by the test harness)
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_limit = 10;
SET param__internal_cascades_cluster_node_count = 4;
-- steer the cost towards the pushed shape: a huge left side with few distinct keys
SET param__internal_join_table_stat_hints = '{"t_abbtk_facts": {"cardinality": 100000000, "avg_row_bytes": 4, "distinct_keys": {"key": 100}}, "t_abbtk_dims": {"cardinality": 1000, "avg_row_bytes": 4, "distinct_keys": {"key": 1000}}}';

-- Only the relative order of the aggregation and the join matters: a `Join` line above an
-- `Aggregating` line means the aggregation was pushed below the join.
SELECT '-- with the bucket-local Top-K conversion the aggregation is still pushed below the join';
SELECT splitByChar(' ', trimLeft(explain))[1] FROM
(
    EXPLAIN SELECT t1.key AS k, count() AS c FROM t_abbtk_facts AS t1 LEFT SEMI JOIN t_abbtk_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
    ORDER BY c DESC LIMIT 5
)
WHERE explain LIKE '%Aggregating%' OR explain LIKE '%Join%';

-- Force two-level states, the shape the bucket-local Top-K conversion serves.
SET max_threads = 4;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT '-- result through cascades';
SELECT t1.key AS k, count() AS c FROM t_abbtk_facts AS t1 LEFT SEMI JOIN t_abbtk_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
ORDER BY c DESC LIMIT 5;

SELECT '-- result classically';
SELECT t1.key AS k, count() AS c FROM t_abbtk_facts AS t1 LEFT SEMI JOIN t_abbtk_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
ORDER BY c DESC LIMIT 5
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_abbtk_facts;
DROP TABLE t_abbtk_dims;
