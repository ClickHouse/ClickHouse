DROP TABLE IF EXISTS pr_tt;
CREATE TABLE pr_tt (k UInt64, v String, blob String) ENGINE=MergeTree() ORDER BY tuple() settings index_granularity=100;
INSERT INTO pr_tt SELECT number, toString(number), repeat('blob_', number % 10) FROM numbers(1_000_000);

-- make sure the optimization is enabled
set enable_analyzer=1, query_plan_optimize_lazy_materialization=true, query_plan_max_limit_for_lazy_materialization=10;
SET enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

select trimLeft(explain) as s from (EXPLAIN
SELECT
    v,
    blob
FROM pr_tt
ORDER BY k ASC
LIMIT 10 settings parallel_replicas_local_plan=1) where s ilike 'LazilyRead%';

SELECT
    v,
    blob
FROM pr_tt
ORDER BY k
LIMIT 10;

DROP TABLE pr_tt;
