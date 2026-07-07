-- Tags: no-replicated-database

-- Regression test for LOGICAL_ERROR "Trying to attach external table to a ready set without explicit elements"
-- When distributed index analysis (DIA) encounters a GLOBAL IN predicate whose set has been built
-- without explicit elements (e.g. because use_index_for_in_with_subqueries_max_values was exceeded),
-- it must gracefully skip the predicate instead of crashing.

DROP TABLE IF EXISTS test_dia_global_in;

CREATE TABLE test_dia_global_in (key Int32, value Int32)
ENGINE = MergeTree()
ORDER BY key
SETTINGS
    distributed_index_analysis_min_parts_to_activate = 0,
    distributed_index_analysis_min_indexes_bytes_to_activate = 0;

SYSTEM STOP MERGES test_dia_global_in;

INSERT INTO test_dia_global_in
SELECT number, number * 10
FROM numbers(10000)
SETTINGS
    max_block_size = 1000,
    min_insert_block_size_rows = 1000,
    max_insert_threads = 1;

-- This query triggers the bug:
-- 1. GLOBAL IN produces a globalIn function in the filter DAG
-- 2. use_index_for_in_with_subqueries_max_values = 1 causes buildOrderedSetInplace to
--    create the set but drop explicit elements (only hashes remain)
-- 3. DIA's tryBuildAdditionalFilterAST finds the globalIn and tries setExternalTable()
-- 4. Without the fix: LOGICAL_ERROR crash. With the fix: predicate is skipped gracefully.
SELECT sum(key)
FROM test_dia_global_in
WHERE key GLOBAL IN (SELECT key FROM test_dia_global_in WHERE key > 5000)
SETTINGS
    distributed_index_analysis_for_non_shared_merge_tree = 1,
    enable_parallel_replicas = 0,
    distributed_index_analysis = 1,
    distributed_index_analysis_only_on_coordinator = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    use_statistics_for_part_pruning = 0,
    use_index_for_in_with_subqueries_max_values = 1,
    send_logs_level = 'fatal';

-- Also verify correctness with parallel replicas enabled
SELECT sum(key)
FROM test_dia_global_in
WHERE key GLOBAL IN (SELECT key FROM test_dia_global_in WHERE key > 5000)
SETTINGS
    automatic_parallel_replicas_mode = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_local_plan = 1,
    use_query_condition_cache = 0,
    distributed_index_analysis_for_non_shared_merge_tree = 1,
    enable_parallel_replicas = 1,
    distributed_index_analysis = 1,
    distributed_index_analysis_only_on_coordinator = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    use_statistics_for_part_pruning = 0,
    use_index_for_in_with_subqueries_max_values = 1,
    send_logs_level = 'fatal';

DROP TABLE test_dia_global_in;
