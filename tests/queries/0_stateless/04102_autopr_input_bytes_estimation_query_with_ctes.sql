-- Tags: stateful

SET use_uncompressed_cache=0;
SET use_query_condition_cache=0;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- Reading of aggregation states from disk will affect `ReadCompressedBytes`
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- External sort spills data to disk and reads it back, which inflates `ReadCompressedBytes`
SET max_bytes_before_external_sort=0, max_bytes_ratio_before_external_sort=0;

-- Override randomized max_threads to avoid timeout on slow builds (ASan)
SET max_threads=0;

set enable_filesystem_cache=1;

-- CTE used as the data source: declare a table in WITH and select from it in the main query.
-- The CTE is inlined, so the main query effectively reads `test.hits` through `cte`.
WITH cte AS (SELECT CounterID, URL FROM test.hits) SELECT COUNT(*) FROM cte WHERE URL LIKE '%google%' FORMAT Null SETTINGS log_comment='04102_query_1';

-- CTE with a filter inside; the main query groups over it.
WITH cte AS (SELECT SearchPhrase, UserID FROM test.hits WHERE SearchPhrase <> '') SELECT SearchPhrase, COUNT(*) AS c FROM cte GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='04102_query_2', max_block_size=65409;

-- CTE used as an IN-filter via an inner SELECT from the CTE.
-- The CTE reads from `numbers()` which does not contribute to `ReadCompressedBytes`,
-- so the estimation should match the bytes read from `test.hits` in the main query.
WITH cte AS (SELECT number FROM numbers(1000)) SELECT COUNT(*) FROM test.hits WHERE CounterID IN (SELECT number FROM cte) FORMAT Null SETTINGS log_comment='04102_query_3';

-- CTE referenced by name directly in the IN clause.
WITH cte AS (SELECT number FROM numbers(1000)) SELECT COUNT(*) FROM test.hits WHERE CounterID IN cte FORMAT Null SETTINGS log_comment='04102_query_4';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Just checking that the estimation is not too far off
SELECT format('{} {} {}', log_comment, compressed_bytes, statistics_input_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['ReadCompressedBytes'] compressed_bytes,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] statistics_input_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE '04102_query_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) > 2;
