-- Tags: no-parallel, no-parallel-replicas

SET allow_experimental_analyzer = 1;

SELECT 'With PREWHERE';

SYSTEM DROP QUERY CONDITION CACHE;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1000000);

SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'],
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true;'
ORDER BY
    event_time_microseconds;

SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'],
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true;'
ORDER BY
    event_time_microseconds;

SELECT 'Without PREWHERE';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'],
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;'
ORDER BY
    event_time_microseconds;

SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'],
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;'
ORDER BY
    event_time_microseconds;

DROP TABLE tab;
