SET max_bytes_ratio_before_external_distinct = 0;

SELECT count() FROM (SELECT DISTINCT number % 1000000 AS k FROM numbers(2000000))
SETTINGS max_bytes_before_external_distinct = '4M', log_comment = '04497_external_distinct_metrics/distinct'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    if(
        any(ProfileEvents['ExternalDistinctWritePart']) >= 1 AND
        any(ProfileEvents['ExternalDistinctMerge']) >= 1 AND
        any(ProfileEvents['ExternalDistinctCompressedBytes']) >= 100000 AND
        any(ProfileEvents['ExternalDistinctUncompressedBytes']) >= 100000 AND
        any(ProfileEvents['ExternalProcessingFilesTotal']) >= 1 AND
        count() == 1,
        'ok',
        'fail: ' || toString(count()) || ' ' || toString(any(ProfileEvents))
    )
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND log_comment = '04497_external_distinct_metrics/distinct' AND type = 'QueryFinish';
