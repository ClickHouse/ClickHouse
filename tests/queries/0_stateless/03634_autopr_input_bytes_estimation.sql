-- Tags: stateful, long

SET use_uncompressed_cache=0;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- Reading of aggregation states from disk will affect `ReadCompressedBytes`
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- Override randomized max_threads to avoid timeout on slow builds (ASan)
SET max_threads=0;

-- settings override: while the total amount of data to be read is small (in order of 100KB), with different settings we may wastefully overread much more data
SELECT COUNT(*) FROM test.hits WHERE AdvEngineID <> 0 FORMAT Null SETTINGS log_comment='query_1',
  remote_filesystem_read_method='threadpool', allow_prefetched_read_pool_for_remote_filesystem=1, filesystem_prefetch_step_marks=0, filesystem_prefetch_step_bytes='100Mi', use_query_condition_cache=0;

-- Unsupported at the moment, refer to comments in `RuntimeDataflowStatisticsCacheUpdater::recordAggregationStateSizes`
-- SELECT COUNT(DISTINCT SearchPhrase) FROM test.hits FORMAT Null SETTINGS log_comment='query_5';

-- With smaller block sizes we can get slightly lower estimations and it will send us over the threshold. Instead of relaxing the check for all queries I prefer to specify the block size here.
SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM test.hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_10', max_block_size=65409;

-- With smaller block sizes we can get slightly lower estimations and it will send us over the threshold. Instead of relaxing the check for all queries I prefer to specify the block size here.
SELECT SearchPhrase, COUNT(*) AS c FROM test.hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_12', max_block_size=65409;

SELECT UserID, COUNT(*) FROM test.hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_15';

SELECT COUNT(*) FROM test.hits WHERE URL LIKE '%google%' FORMAT Null SETTINGS log_comment='query_20';

SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM test.hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_21';

SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM test.hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_22';

SELECT * FROM test.hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS log_comment='query_23';

SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM test.hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25 FORMAT Null SETTINGS log_comment='query_28';

SELECT 1, URL, COUNT(*) AS c FROM test.hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_34';

-- For some reason, with smaller block sizes `ReadCompressedBytes` shows twice the size of `CounterID` column for query_43
SET max_block_size=65409;

-- Just checking that statistics are collected with read in order
SELECT CounterID from test.hits ORDER BY CounterID DESC FORMAT Null SETTINGS optimize_read_in_order=1, query_plan_read_in_order=1, log_comment='query_43';

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
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) > 2;

