-- The cascades `AggregationPushdown` transformation must not push an aggregation below a join
-- when the top-K threshold merge is enabled on it (`Aggregator::Params::threshold_top_k`): the
-- merge keeps only each bucket's best groups, which is exact only when the aggregation directly
-- feeds its `ORDER BY ... LIMIT`. Below a `LEFT SEMI` join it would prune groups before the join
-- filters them, so the winners that have no match on the right side would push the true answer
-- out. The same restriction already applies to the conversion-stage selection (`bucket_top_k`).

DROP TABLE IF EXISTS t_abtk_facts;
DROP TABLE IF EXISTS t_abtk_dims;

CREATE TABLE t_abtk_facts (key UInt32, value Int64) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_abtk_dims (key UInt32) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_abtk_facts;
SYSTEM STOP MERGES t_abtk_dims;

-- 100000 keys with max(value) = key; only the lower half has a match on the right side, so the
-- best groups of every bucket before the join are all unmatched.
INSERT INTO t_abtk_facts SELECT number % 100000, number % 100000 FROM numbers(200000);
INSERT INTO t_abtk_dims SELECT number FROM numbers(50000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET cascades_aggregation_pushdown = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_aggregation_bucket_top_k = 1;
-- the pre-cascades join-order pass attaches the row estimates the cost model needs (both
-- settings are randomized by the test harness)
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_limit = 10;
SET param__internal_cascades_cluster_node_count = 4;
-- steer the cost towards the pushed shape: a huge left side with few distinct keys
SET param__internal_join_table_stat_hints = '{"t_abtk_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_abtk_dims": {"cardinality": 1000, "avg_row_bytes": 4, "distinct_keys": {"key": 1000}}}';

-- Only the relative order of the aggregation and the join matters: a `Join` line above an
-- `Aggregating` line means the aggregation was pushed below the join.
SELECT '-- canary: without ORDER BY ... LIMIT the aggregation is pushed below the join';
SELECT splitByChar(' ', trimLeft(explain))[1] FROM
(
    EXPLAIN SELECT t1.key AS k, max(t1.value) AS m FROM t_abtk_facts AS t1 LEFT SEMI JOIN t_abtk_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
)
WHERE explain LIKE '%Aggregating%' OR explain LIKE '%Join%';

SELECT '-- with the top-K threshold merge the aggregation stays above the join';
SELECT splitByChar(' ', trimLeft(explain))[1] FROM
(
    EXPLAIN SELECT t1.key AS k, max(t1.value) AS m FROM t_abtk_facts AS t1 LEFT SEMI JOIN t_abtk_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
    ORDER BY m DESC LIMIT 5
)
WHERE explain LIKE '%Aggregating%' OR explain LIKE '%Join%';

-- Force two-level states merged on several threads, the shape the threshold merge serves.
SET max_threads = 4;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT '-- result through cascades';
SELECT t1.key AS k, max(t1.value) AS m FROM t_abtk_facts AS t1 LEFT SEMI JOIN t_abtk_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
ORDER BY m DESC LIMIT 5;

SELECT '-- result classically';
SELECT t1.key AS k, max(t1.value) AS m FROM t_abtk_facts AS t1 LEFT SEMI JOIN t_abtk_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
ORDER BY m DESC LIMIT 5
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_abtk_facts;
DROP TABLE t_abtk_dims;
