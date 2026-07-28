-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache

 -- w/o local plan for parallel replicas the test will fail in ParallelReplicas CI run since filter steps will be executed as part of remote queries
set parallel_replicas_local_plan=1;

SET allow_experimental_analyzer = 1;

-- Tests that queries with enabled query condition cache correctly populate profile events

SELECT '--- with move to PREWHERE';

SYSTEM DROP QUERY CONDITION CACHE;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1_000_000); -- 1 mio rows sounds like a lot but the QCC doesn't cache anything for less data

SELECT count(*) FROM tab WHERE b = 10_000 FORMAT Null SETTINGS use_query_condition_cache = true;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'],
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count(*) FROM tab WHERE b = 10_000 FORMAT Null SETTINGS use_query_condition_cache = true;'
ORDER BY
    event_time_microseconds;

SELECT * FROM tab WHERE b = 10_000 FORMAT Null SETTINGS use_query_condition_cache = true;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'],
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM tab WHERE b = 10_000 FORMAT Null SETTINGS use_query_condition_cache = true;'
ORDER BY
    event_time_microseconds;

SELECT '--- without move to PREWHERE';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT count(*) FROM tab WHERE b = 10_000 FORMAT Null SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'],
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count(*) FROM tab WHERE b = 10_000 FORMAT Null SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;'
ORDER BY
    event_time_microseconds;

SELECT * FROM tab WHERE b = 10_000 FORMAT Null SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'],
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM tab WHERE b = 10_000 FORMAT Null SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;'
ORDER BY
    event_time_microseconds;

DROP TABLE tab;
