-- A join runtime filter must reach the parallel-replicas local plan, the same way an ordinary filter
-- does, instead of being stranded above the aggregation as a separate `Filter` step.

DROP TABLE IF EXISTS pr_rf_probe;
DROP TABLE IF EXISTS pr_rf_build;

CREATE TABLE pr_rf_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO pr_rf_probe SELECT number, number FROM numbers(100000);

CREATE TABLE pr_rf_build (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO pr_rf_build SELECT number FROM numbers(10);

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET enable_join_runtime_filters = 1;
SET parallel_replicas_filter_pushdown = 1;
-- Keep the small table on the build side so the plan shape below is stable.
SET query_plan_join_swap_table = false;

SELECT replaceAll(replaceRegexpOne(explain, '^[^A-Za-z]*', ''), currentDatabase(), 'default') AS step
FROM (
    EXPLAIN actions = 1
    SELECT sum(agg.s)
    FROM (SELECT k, sum(v) AS s FROM pr_rf_probe GROUP BY k) AS agg
    JOIN pr_rf_build AS b ON agg.k = b.k
)
WHERE explain LIKE '%Aggregating%'
   OR explain LIKE '%ReadFromMergeTree%'
   OR explain LIKE '%Runtime filters:%'
   OR explain LIKE '%Filter column:%';

SELECT sum(agg.s)
FROM (SELECT k, sum(v) AS s FROM pr_rf_probe GROUP BY k) AS agg
JOIN pr_rf_build AS b ON agg.k = b.k;

DROP TABLE pr_rf_probe;
DROP TABLE pr_rf_build;
