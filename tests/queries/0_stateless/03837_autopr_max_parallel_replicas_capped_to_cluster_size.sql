-- Verify that max_parallel_replicas in the automatic parallel replicas optimization
-- is capped to the actual cluster size, not the raw setting value.

DROP TABLE IF EXISTS t;

CREATE TABLE t(key String, value UInt64) ENGINE = MergeTree ORDER BY tuple();

-- max_parallel_replicas=100 is intentionally much larger than the cluster size (3 replicas)
SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=100, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

INSERT INTO t SELECT 'key' || toString(number % 10), number FROM numbers(5e5);

-- First query: collect statistics (cache is empty)
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03837_autopr_max_parallel_replicas_capped_query_0';

-- Second query: statistics available, formula is evaluated using effective num_replicas
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03837_autopr_max_parallel_replicas_capped_query_1';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log, text_log;

SET max_rows_to_read = 0;

-- The optimizer logs "The applied formula: {input_bytes} / {max_threads} ? ({input_bytes} / ({max_threads} * {num_replicas}) + ...)"
-- Extract num_replicas from the formula and verify it equals 3 (cluster size), not 100 (setting).
SELECT DISTINCT toUInt64(extract(message, '\\(\\d+ \\* (\\d+)\\)')) AS num_replicas_in_formula
FROM system.text_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (logger_name = 'optimizeTree') AND (message LIKE 'The applied formula:%') AND (query_id IN (
    SELECT query_id
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment = '03837_autopr_max_parallel_replicas_capped_query_1') AND (type = 'QueryFinish')
));

DROP TABLE t;
