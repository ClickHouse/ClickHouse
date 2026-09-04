-- Compare in-memory and external aggregation on deterministic generated data.
-- The original full scans of `test.hits` took more than 300 seconds under TSan
-- when 25 tests shared the server.

SET log_queries = 1;
SET max_execution_time = 30;
SET max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;

SELECT URL, uniq(SearchPhrase) AS u
FROM
(
    SELECT toString(number % 10000) AS URL, toString(number) AS SearchPhrase
    FROM numbers(100000)
)
GROUP BY URL
ORDER BY u DESC, URL
LIMIT 10
SETTINGS max_bytes_before_external_group_by = 0, max_threads = 2;

SELECT URL, uniq(SearchPhrase) AS u
FROM
(
    SELECT toString(number % 10000) AS URL, toString(number) AS SearchPhrase
    FROM numbers(100000)
)
GROUP BY URL
ORDER BY u DESC, URL
LIMIT 10
SETTINGS
    max_bytes_before_external_group_by = '64Ki',
    max_threads = 2,
    aggregation_memory_efficient_merge_threads = 1,
    log_comment = '00084_external_aggregation_spill';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['ExternalAggregationWritePart'] > 0,
    ProfileEvents['ExternalAggregationMerge'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '00084_external_aggregation_spill'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;
