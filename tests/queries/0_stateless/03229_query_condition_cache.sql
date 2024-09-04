-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CONDITION CACHE;
DROP TABLE IF EXISTS system.query_log SYNC;

DROP TABLE IF EXISTS tab SYNC;
CREATE TABLE tab (`a` Int64, `b` Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1000000);

SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true;
SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true;

-- PREWHERE condition will hit the query condition cache.
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryConditionCacheHits'], ProfileEvents['QueryConditionCacheMisses'], toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query IN ('SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true;',
                'SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true;')
order by event_time_microseconds;

SELECT 'Test optimize_move_to_prewhere=false';
SYSTEM DROP QUERY CONDITION CACHE;

SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere=false;
SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere=false;

-- WHERE condition will hit the query condition cache.
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryConditionCacheHits'], ProfileEvents['QueryConditionCacheMisses'], toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query IN ('SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere=false;',
               'SELECT * FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere=false;');

DROP TABLE IF EXISTS tab;

