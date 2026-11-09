-- Tags: no-random-settings, no-random-merge-tree-settings

-- Verify that during distributed index analysis a WHERE predicate and the SAMPLE
-- clause are combined (filter_ast AND sampling_filter) and forwarded together to the
-- replicas, exercising the makeASTForLogicalAnd branch in DistributedIndexAnalyzer
-- (src/Interpreters/ClusterProxy/distributedIndexAnalysis.cpp:267-268). Without a
-- WHERE the else-if branch just forwards the sampling_filter.
--
-- The regression guard reads the query the coordinator actually sends to the
-- replicas from system.query_log (the mergeTreeAnalyzeIndexesUUID(...) call). That
-- text is a formatted filter AST, so it is independent of build type and part
-- layout, unlike an initiator-side EXPLAIN plan.
SET send_logs_level = 'error';

DROP TABLE IF EXISTS t_dia_sampling;

-- The minmax skip index on `value` makes the WHERE predicate index-usable, so
-- getFilterAST keeps it and it becomes part of the filter forwarded to the replicas.
-- Without an index on `value`, the WHERE column is not index-usable and is dropped.
CREATE TABLE t_dia_sampling (key UInt64, value UInt64, INDEX idx_value value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY intHash32(key) SAMPLE BY intHash32(key)
SETTINGS index_granularity = 256,
  min_bytes_for_wide_part = '1G',
  index_granularity_bytes = '10M',
  distributed_index_analysis_min_parts_to_activate = 0,
  distributed_index_analysis_min_indexes_bytes_to_activate = 0;

SYSTEM STOP MERGES t_dia_sampling;
INSERT INTO t_dia_sampling SELECT number, number FROM numbers(100000) SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;
INSERT INTO t_dia_sampling SELECT number + 100000, number FROM numbers(100000) SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;

SET cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas';
SET max_parallel_replicas = 2;
SET use_query_condition_cache = 0;
SET allow_experimental_parallel_reading_from_replicas = 0;
SET allow_experimental_analyzer = 1;
-- Force distributed index analysis to fire on a plain (non-shared) MergeTree so the
-- combine branch is actually reached regardless of the default storage policy.
SET distributed_index_analysis_for_non_shared_merge_tree = 1;

-- Results must be identical with and without distributed index analysis.
SELECT count() FROM t_dia_sampling SAMPLE 0.1 SETTINGS distributed_index_analysis = 0;
SELECT count() FROM t_dia_sampling SAMPLE 0.1 SETTINGS distributed_index_analysis = 1;

-- SAMPLE + WHERE exercises the AND-combination branch (filter_ast && sampling_filter).
SELECT count() FROM t_dia_sampling SAMPLE 0.1 WHERE value < 50000 SETTINGS distributed_index_analysis = 0;
SELECT count() FROM t_dia_sampling SAMPLE 0.1 WHERE value < 50000 SETTINGS distributed_index_analysis = 1;

SYSTEM FLUSH LOGS query_log;

-- Inspect the filter forwarded to the replicas by distributed index analysis, scoped
-- to this table's UUID so other tests do not interfere. The forwarded filter is the
-- second argument of mergeTreeAnalyzeIndexesUUID(...):
--   * SAMPLE + WHERE -> carries BOTH the WHERE predicate less(value, ...) and the
--     sampling predicate less(intHash32(key), ...): the AND-combine forwarded both.
--   * SAMPLE only    -> carries less(intHash32(key), ...) but NOT less(value, ...).
-- The forwarded remote queries run with current_database = 'default', so they are
-- scoped to this test via initial_query_id: their initiator queries are the ones that
-- ran in currentDatabase(). This isolates the assertion from other tests' query_log.
WITH
    (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_dia_sampling') AS tuuid,
    (SELECT groupArray(query_id) FROM system.query_log
        WHERE current_database = currentDatabase() AND is_initial_query = 1 AND type = 'QueryFinish'
          AND event_date >= today() - 1) AS initiators
SELECT
    countIf(query ILIKE '%less(value,%' AND query ILIKE '%intHash32(key)%') > 0 AS combined_filter_forwarded,
    countIf(query NOT ILIKE '%less(value,%' AND query ILIKE '%intHash32(key)%') > 0 AS sampling_only_forwarded
FROM system.query_log
WHERE is_initial_query = 0
  AND type = 'QueryFinish'
  AND has(initiators, initial_query_id)
  AND query ILIKE '%mergeTreeAnalyzeIndexesUUID%'
  AND query ILIKE concat('%', tuuid, '%')
  AND event_date >= today() - 1;

DROP TABLE t_dia_sampling;
