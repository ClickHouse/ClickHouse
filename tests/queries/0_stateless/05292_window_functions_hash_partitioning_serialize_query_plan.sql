-- Tags: distributed

-- A window with `query_plan_window_functions_hash_partitioning` in a query plan serialized for a single remote
-- shard (`serialize_query_plan`): the shard must compute it with `PartitionAggregateTransform` and keep the
-- spill settings, and the results must not change.

-- A local shard gets a local plan, which is not serialized.
SET prefer_localhost_replica = 0, enable_parallel_replicas = 0;
-- Hash partitioning is not used when the storage ordering may be reused for the window sort.
SET query_plan_reuse_storage_ordering_for_window_functions = 0;

DROP TABLE IF EXISTS t_hash_window_serialize;
CREATE TABLE t_hash_window_serialize (k Int32, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_hash_window_serialize SELECT number % 7, number FROM numbers(1000);

SELECT count(), sum(cityHash64(*)) FROM (SELECT x, sum(x) OVER (PARTITION BY k) FROM remote('127.0.0.2', currentDatabase(), t_hash_window_serialize))
SETTINGS serialize_query_plan = 0, query_plan_window_functions_hash_partitioning = 0, log_comment = '05292_sort';

SELECT count(), sum(cityHash64(*)) FROM (SELECT x, sum(x) OVER (PARTITION BY k) FROM remote('127.0.0.2', currentDatabase(), t_hash_window_serialize))
SETTINGS serialize_query_plan = 1, query_plan_window_functions_hash_partitioning = 1, log_comment = '05292_hash';

SELECT count(), sum(cityHash64(*)) FROM (SELECT x, sum(x) OVER (PARTITION BY k) FROM remote('127.0.0.2', currentDatabase(), t_hash_window_serialize))
SETTINGS serialize_query_plan = 1, query_plan_window_functions_hash_partitioning = 1, log_comment = '05292_spill',
    max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0, max_block_size = 10;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- The window processors of the shard queries.
SELECT log_comment, arraySort(groupUniqArray(name))
FROM system.processors_profile_log AS p
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND is_initial_query AND log_comment LIKE '05292_%'
) AS q ON p.initial_query_id = q.query_id
WHERE p.query_id != p.initial_query_id AND name IN ('PartitionAggregateTransform', 'WindowTransform')
GROUP BY log_comment
ORDER BY log_comment;

-- The shard query spills only with the spill settings.
SELECT log_comment, sum(ProfileEvents['ExternalProcessingFilesTotal']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query AND query_kind = 'Select'
    AND initial_query_id IN
    (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
            AND is_initial_query AND log_comment LIKE '05292_%'
    )
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_hash_window_serialize;
