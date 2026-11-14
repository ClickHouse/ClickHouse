-- With parallel replicas, the aggregated data read from a `MergeTree` table is merged as mergeable state, in the
-- memory-efficient mode when `distributed_aggregation_memory_efficient` is enabled. An `Alias` of the table is merged
-- in the same mode.

DROP TABLE IF EXISTS pr_alias;
DROP TABLE IF EXISTS pr_source;

CREATE TABLE pr_source (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE pr_alias ENGINE = Alias(pr_source);
INSERT INTO pr_source SELECT number % 100, number FROM numbers(1000);

SET explain_query_plan_default = 'legacy';
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
-- The plan-based implementation of parallel replicas always merges in the other mode.
SET parallel_replicas_plan_based = 0;
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0,
    optimize_aggregation_in_order = 0;

SET enable_analyzer = 1;
SELECT 'analyzer', 'source', countIf(explain LIKE '%Mode: memory-efficient%') FROM (EXPLAIN actions = 1 SELECT a, sum(b) FROM pr_source GROUP BY a);
SELECT 'analyzer', 'alias', countIf(explain LIKE '%Mode: memory-efficient%') FROM (EXPLAIN actions = 1 SELECT a, sum(b) FROM pr_alias GROUP BY a);
SELECT 'analyzer', 'result', count(), sum(s) FROM (SELECT a, sum(b) AS s FROM pr_alias GROUP BY a);

SET enable_analyzer = 0;
SELECT 'old analyzer', 'source', countIf(explain LIKE '%Mode: memory-efficient%') FROM (EXPLAIN actions = 1 SELECT a, sum(b) FROM pr_source GROUP BY a);
SELECT 'old analyzer', 'alias', countIf(explain LIKE '%Mode: memory-efficient%') FROM (EXPLAIN actions = 1 SELECT a, sum(b) FROM pr_alias GROUP BY a);
SELECT 'old analyzer', 'result', count(), sum(s) FROM (SELECT a, sum(b) AS s FROM pr_alias GROUP BY a);

DROP TABLE pr_alias;
DROP TABLE pr_source;
