-- The partial aggregation under a Cascades merge per bucket sends its states on from all of its streams.
-- It promises bucket order only to the memory-efficient merge on one node, which shares its memo group,
-- and with the promise it would send its whole result from one stream.

DROP TABLE IF EXISTS t_shuffle_merge_unordered;
CREATE TABLE t_shuffle_merge_unordered (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '', index_granularity = 1024;
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_shuffle_merge_unordered;
INSERT INTO t_shuffle_merge_unordered SELECT number % 5000, number FROM numbers(1000000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
SET distributed_plan_partial_aggregation_before_shuffle = 1;
SET distributed_plan_force_shuffle_aggregation = 0;
-- The bucket order is promised for the memory-efficient merge.
SET distributed_aggregation_memory_efficient = 1;
SET distributed_plan_default_reader_bucket_count = 2;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
SET param__internal_cascades_cluster_node_count = 2;
SET param__internal_join_table_stat_hints = '{"t_shuffle_merge_unordered": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 10000000}}}';
-- Several read streams in every reading task, so that the number of streams after the partial
-- aggregation shows whether it sends its result from one stream.
SET max_threads = 4;
SET merge_tree_min_rows_for_concurrent_read = 1000;
SET merge_tree_min_bytes_for_concurrent_read = 1;

SELECT '-- the merge per bucket over shuffled states';
EXPLAIN SELECT count() FROM (SELECT k, sum(v) FROM t_shuffle_merge_unordered GROUP BY k);

SELECT '-- results match the local execution';
SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_shuffle_merge_unordered GROUP BY k) SETTINGS make_distributed_plan = 0;
SELECT count(), sum(s) FROM (SELECT k, sum(v) AS s FROM t_shuffle_merge_unordered GROUP BY k)
SETTINGS log_processors_profiles = 1, log_comment = '05233_shuffle_merge';

-- The log queries below are not the subject of the test; they run without the distributed plan, which
-- would read the system tables on the workers while they still merge their parts.
SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT '-- the reading tasks send the states on from more than one stream each';
-- The shuffle sender scatters every output stream of the partial aggregation separately.
SELECT countIf(name = 'ScatterByPartitionTransform') > uniqExact(query_id)
FROM system.processors_profile_log
WHERE initial_query_id = (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND log_comment = '05233_shuffle_merge' AND is_initial_query AND type = 'QueryFinish'
        ORDER BY event_time DESC LIMIT 1)
    AND query_id LIKE '%::stage_0%' AND event_date >= yesterday();

DROP TABLE t_shuffle_merge_unordered;
