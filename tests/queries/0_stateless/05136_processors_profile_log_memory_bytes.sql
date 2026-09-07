-- `memory_allocated_bytes` and `memory_freed_bytes` are collected from the thread allocation counters
-- around `IProcessor::work`, so run on a single thread and without untracked memory.
SELECT uniqExact(toString(number))
FROM numbers(100000)
SETTINGS
    log_processors_profiles = 1,
    log_queries = 1,
    log_queries_min_type = 'QUERY_FINISH',
    log_comment = '05136_processors_profile_log_memory_bytes',
    max_threads = 1,
    max_untracked_memory = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE event_date >= yesterday()
            AND event_time >= now() - 600
            AND current_database = currentDatabase()
            AND log_comment = '05136_processors_profile_log_memory_bytes'
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    ) AS query_id_
-- `AggregatingTransform` builds the hash table and frees the consumed chunks inside its `work`.
SELECT
    sum(memory_allocated_bytes) > 0,
    sum(memory_freed_bytes) > 0
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND query_id = query_id_
    AND name = 'AggregatingTransform';
