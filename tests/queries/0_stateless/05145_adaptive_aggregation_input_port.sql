SET max_threads = 2;
SET max_block_size = 4096;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 128;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET log_processors_profiles = 1;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';

-- Partitioning receives the forwarded blocks' aggregate arguments at their original row count; grouping-only
-- columns stay local. Each producer's first block freezes its table in the checks after the block and
-- records no misses, so it is not forwarded: two producers forward six of the eight blocks.
SELECT number AS k, count()
FROM numbers_mt(32768) GROUP BY k
SETTINGS log_comment = 'adaptive_input_port_count' FORMAT Null;

SELECT number AS k, sum(number % 7), max(toLowCardinality(toString(number % 10))), min(toUInt64(1))
FROM numbers_mt(32768) GROUP BY k
SETTINGS log_comment = 'adaptive_input_port_arguments' FORMAT Null;

SELECT number AS k
FROM numbers_mt(32768) GROUP BY k
SETTINGS log_comment = 'adaptive_input_port_keys' FORMAT Null;

-- A table that never freezes forwards nothing; its staging chain only sees completion.
SELECT toUInt64(number % 4) AS k, count()
FROM numbers_mt(32768) GROUP BY k
SETTINGS log_comment = 'adaptive_input_port_learning' FORMAT Null;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT
    q.log_comment,
    sumIf(p.input_rows, p.name = 'AdaptiveAggregationPartitionTransform'),
    sumIf(p.output_rows, p.name = 'AdaptiveAggregationPublishTransform'),
    uniqExact(p.name) = 3,
    sumIf(p.output_rows, p.name = 'AdaptiveAggregationPartitionTransform')
        = sumIf(p.input_rows, p.name = 'AdaptiveAggregationCoalescingTransform'),
    sumIf(p.output_rows, p.name = 'AdaptiveAggregationCoalescingTransform')
        = sumIf(p.input_rows, p.name = 'AdaptiveAggregationPublishTransform'),
    (sumIf(p.input_bytes, p.name = 'AdaptiveAggregationPartitionTransform') > 0)
        = (q.log_comment = 'adaptive_input_port_arguments'),
    max(q.ProfileEvents['AdaptiveAggregationStagedRecords'] > 0) = (q.log_comment != 'adaptive_input_port_learning')
FROM system.processors_profile_log AS p
INNER JOIN system.query_log AS q ON p.query_id = q.query_id
WHERE p.name IN ('AdaptiveAggregationPartitionTransform', 'AdaptiveAggregationCoalescingTransform',
    'AdaptiveAggregationPublishTransform')
    AND q.type = 'QueryFinish'
    AND q.current_database = currentDatabase()
    AND q.event_date >= yesterday() AND q.event_time >= now() - 600
    AND q.log_comment LIKE 'adaptive_input_port_%'
GROUP BY q.log_comment
ORDER BY q.log_comment;
