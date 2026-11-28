-- Tags: distributed

SET max_threads = 1;
SET max_block_size = 128;
SET max_bytes_before_external_limit_by = 1;
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_untracked_memory = 0;
SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 1;
SET use_hedged_requests = 0;
SET distributed_group_by_no_merge = 0;
SET log_queries = 1;
SET log_processors_profiles = 1;

-- A single remote shard executes the complete serialized plan. The `LIMIT BY` key is not a prefix of the sort
-- key, which keeps the `LIMIT BY` above the sort on the hash-based path, and the one-byte threshold forces it
-- to spill. Without arrival-order restoration, the spill merge returns the second row of each key in key
-- order and the final `LIMIT` selects the wrong prefix.
SELECT k, v
FROM remote('127.0.0.1', view(SELECT number % 2048 AS k, number AS v FROM numbers(16384)))
ORDER BY v DESC
LIMIT 1 OFFSET 1 BY k
LIMIT 16
SETTINGS log_comment = '05344_external_limit_by_remote_order';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- Order restoration creates a `MergeSortingTransform` owned by the remote `LimitBy` step. The initiator must
-- execute no row-sorting processor, so it cannot repair unordered rows from the secondary. Restoring
-- transport chunk sequence preserves the source stream's row order and does not count as sorting.
SELECT
    if(query_id = initial_query_id, 'initiator', 'remote') AS query_role,
    countIf(name = 'ExternalLimitByTransform' AND output_rows > 0) > 0 AS external_limit_by,
    countIf(plan_step_name = 'LimitBy' AND name = 'MergeSortingTransform' AND output_rows > 0) > 0 AS restored_order,
    countIf(name LIKE '%SortingTransform' OR name = 'MergingSortedTransform') = 0 AS no_sorting
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND initial_query_id =
    (
        SELECT argMax(query_id, event_time_microseconds)
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
            AND is_initial_query AND current_database = currentDatabase()
            AND log_comment = '05344_external_limit_by_remote_order'
    )
GROUP BY query_role
ORDER BY query_role;
