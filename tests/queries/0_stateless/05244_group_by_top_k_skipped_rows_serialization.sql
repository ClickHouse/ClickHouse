-- Blocks whose rows the full top-K heap mostly skips must not serialize all their keys up front. Only that batch
-- path allocates `serialized_keys`: 16 bytes per row, so exactly 80048 bytes per 5003-row block.

SET serialize_query_plan = 0, enable_parallel_replicas = 0, log_queries = 1;
SET enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_rows_to_group_by = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET max_block_size = 5003, memory_profiler_sample_probability = 1;
SET memory_profiler_sample_min_allocation_size = 80048, memory_profiler_sample_max_allocation_size = 80048;

-- The winning groups get rows from the first, batch-serialized block and from the per-row blocks after it.
SELECT a, b, count()
FROM (SELECT if(number % 97 = 0, number % 5, 1000 + intHash64(number) % 1000000) AS a, if(number % 11 = 0, NULL, toString(number % 3)) AS b FROM numbers(500300))
GROUP BY a, b ORDER BY a, b LIMIT 10
SETTINGS max_threads = 1, log_comment = 'skipped';

-- The heap skips the first 50 blocks and admits every row of the last 50, which must take the batch path again.
SELECT a, b, count()
FROM (SELECT if(number < 250150, 1000000000 + intHash64(number) % 1000000000, toUInt64(1000000000 - number)) AS a, toString(number % 3) AS b FROM numbers(500300))
GROUP BY a, b ORDER BY a, b LIMIT 10
SETTINGS max_threads = 1, log_comment = 'admitted after skipped';

SELECT a, b, count()
FROM (SELECT if(number % 97 = 0, number % 5, 1000 + intHash64(number) % 1000000) AS a, toString(number % 3) AS b FROM numbers_mt(160096))
GROUP BY a, b ORDER BY a, b LIMIT 10
SETTINGS max_threads = 2, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 5000,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000, log_comment = 'adaptive learning';

SYSTEM FLUSH LOGS query_log, trace_log;

SELECT log_comment, batch_blocks < 10, batch_blocks BETWEEN 10 AND 90, learning_gave_up
FROM
(
    SELECT q.log_comment AS log_comment, countIf(t.size = 80048) AS batch_blocks, any(q.ProfileEvents['AdaptiveAggregationGiveUps']) > 0 AS learning_gave_up
    FROM system.query_log AS q
    LEFT JOIN (SELECT query_id, size FROM system.trace_log WHERE event_date >= yesterday() AND trace_type = 'MemorySample' AND size = 80048) AS t USING (query_id)
    WHERE q.event_date >= yesterday() AND q.current_database = currentDatabase() AND q.type = 'QueryFinish'
        AND q.log_comment IN ('skipped', 'admitted after skipped', 'adaptive learning')
    GROUP BY q.log_comment
)
ORDER BY log_comment;
