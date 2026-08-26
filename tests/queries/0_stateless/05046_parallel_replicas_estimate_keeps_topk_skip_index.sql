-- The parallel replicas row estimate ran index analysis before `tryOptimizeTopK` and left its indexes on
-- the step, so `applyFilters` had nothing to rebuild and the executed read lost the TopK skip index.
-- The outer settings only keep the test runner from enabling parallel replicas for the whole query,
-- and the disabled query condition cache picks the estimate path that used to cache on the step.

CREATE TABLE t_05046 (k UInt64, v UInt64, INDEX v_mm v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;

INSERT INTO t_05046 SELECT number, number FROM numbers(1000000);

SELECT countIf(explain LIKE '%Filter TopK Granules%') AS topk_skip_index_used
FROM
(
    EXPLAIN indexes = 1
    SELECT k, v FROM t_05046 ORDER BY v ASC LIMIT 10
    SETTINGS enable_parallel_replicas = 1,
             parallel_replicas_for_non_replicated_merge_tree = 1,
             cluster_for_parallel_replicas = 'parallel_replicas',
             max_parallel_replicas = 3,
             parallel_replicas_local_plan = 1,
             parallel_replicas_min_number_of_rows_per_replica = 1000,
             use_skip_indexes_for_top_k = 1,
             use_top_k_dynamic_filtering = 1,
             use_query_condition_cache = 0,
             query_plan_max_limit_for_top_k_optimization = 100000
)
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE t_05046;
