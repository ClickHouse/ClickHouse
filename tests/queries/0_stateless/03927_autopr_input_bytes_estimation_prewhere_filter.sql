-- Tags: stateful, long

SET use_uncompressed_cache=0;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- Reading of aggregation states from disk will affect `ReadCompressedBytes`
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- To avoid too slow test execution
set remote_filesystem_read_method='threadpool', allow_prefetched_read_pool_for_remote_filesystem=1, filesystem_prefetch_step_marks=0, filesystem_prefetch_step_bytes='100Mi';

-- Override randomized max_threads to avoid timeout on slow builds (ASan)
SET max_threads=0;

SELECT URL FROM test.hits WHERE WatchID < 4611892230367380000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_0';
SELECT URL FROM test.hits WHERE WatchID < 5550265976347679000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_1';
SELECT URL FROM test.hits WHERE WatchID < 6509275139329711000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_2';

SELECT CounterID, URL FROM test.hits WHERE WatchID < 4611892230367380000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_10';
SELECT CounterID, URL FROM test.hits WHERE WatchID < 5550265976347679000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_11';
SELECT CounterID, URL FROM test.hits WHERE WatchID < 6509275139329711000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_12';

SELECT CounterID, URL, Referer FROM test.hits WHERE WatchID < 4611892230367380000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_20';
SELECT CounterID, URL, Referer FROM test.hits WHERE WatchID < 5550265976347679000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_21';
SELECT CounterID, URL, Referer FROM test.hits WHERE WatchID < 6509275139329711000 FORMAT Null SETTINGS log_comment='03927_autopr_input_bytes_estimation_prewhere_filter_22';

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
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE '03927_autopr_input_bytes_estimation_prewhere_filter_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) > 2;
