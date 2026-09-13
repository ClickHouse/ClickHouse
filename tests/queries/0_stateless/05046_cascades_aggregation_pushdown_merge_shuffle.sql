-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- Unlike the `MergingAggregatedStep` it replaced, the merge-only `Aggregating` above the pushed
-- variant-A join participates in the Cascades distribution strategies, so a Shuffle merge is now
-- expressible: repartition the state rows by the `GROUP BY` keys, merge per node, gather the
-- (already merged, much smaller) result above. With `exchange_fixed_overhead = 0` the Shuffle
-- merge beats the Local one (its per-node merge is priced over 1/N of the rows and the extra
-- exchanges cost no fixed overhead), so the shape below is pinned and executed.

DROP TABLE IF EXISTS t_ms_facts;
DROP TABLE IF EXISTS t_ms_dims;

CREATE TABLE t_ms_facts (key UInt32, value Int64) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_ms_dims (key UInt32, name String) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_ms_facts;
SYSTEM STOP MERGES t_ms_dims;

INSERT INTO t_ms_facts SELECT number % 10, number FROM numbers(1000);
INSERT INTO t_ms_dims SELECT number, concat('name_', toString(number)) FROM numbers(8);

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
SET param__internal_cascades_cost_config = '{"exchange_fixed_overhead":0}';
SET param__internal_join_table_stat_hints = '{"t_ms_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_ms_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

-- Shuffle-merge evidence, pinned as the full legacy EXPLAIN: the variant-A sandwich (the
-- merge-only `Aggregating` above the `JoinLogical`, the partial below it), a `ShuffleExchange`
-- BELOW the merge (the merge consumes repartitioned data instead of a gathered stream) and the
-- lone `GatherExchange` ABOVE the merge (only the merged result is gathered). The Local merge
-- would show the gather below the merge and no shuffle.
SELECT '-- canary: variant A with a Shuffle merge (shuffle below the merge, gather above it)';
EXPLAIN SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_ms_facts AS t1 LEFT JOIN t_ms_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy';

SELECT '-- execution through the Shuffle merge';
SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_ms_facts AS t1 LEFT JOIN t_ms_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;

SELECT '-- the same execution without the distributed planner must match';
SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_ms_facts AS t1 LEFT JOIN t_ms_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_ms_facts;
DROP TABLE t_ms_dims;
