SET enable_analyzer = 1;

WITH
    ( SELECT sleep(0.0001) FROM system.one ) as a1,
    ( SELECT sleep(0.0001) FROM system.one ) as a2,
    ( SELECT sleep(0.0001) FROM system.one ) as a3,
    ( SELECT sleep(0.0001) FROM system.one ) as a4,
    ( SELECT sleep(0.0001) FROM system.one ) as a5
SELECT '02177_CTE_GLOBAL_ON', a1, a2, a3, a4, a5 FROM system.numbers LIMIT 100
FORMAT Null
SETTINGS enable_global_with_statement = 1;

WITH
    ( SELECT sleep(0.0001) FROM system.one ) as a1,
    ( SELECT sleep(0.0001) FROM system.one ) as a2,
    ( SELECT sleep(0.0001) FROM system.one ) as a3,
    ( SELECT sleep(0.0001) FROM system.one ) as a4,
    ( SELECT sleep(0.0001) FROM system.one ) as a5
SELECT '02177_CTE_GLOBAL_OFF', a1, a2, a3, a4, a5 FROM system.numbers LIMIT 100
        FORMAT Null
SETTINGS enable_global_with_statement = 0;

WITH
    ( SELECT sleep(0.0001) FROM system.one ) as a1,
    ( SELECT sleep(0.0001) FROM system.one ) as a2,
    ( SELECT sleep(0.0001) FROM system.one ) as a3,
    ( SELECT sleep(0.0001) FROM system.one ) as a4,
    ( SELECT sleep(0.0001) FROM system.one ) as a5
SELECT '02177_CTE_NEW_ANALYZER', a1, a2, a3, a4, a5 FROM system.numbers LIMIT 100
        FORMAT Null
SETTINGS enable_analyzer = 1;

SYSTEM FLUSH LOGS;
SELECT
       '02177_CTE_GLOBAL_ON',
       ProfileEvents['SleepFunctionCalls'] as sleep_calls,
       ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds,
       ProfileEvents['ScalarSubqueriesGlobalCacheHit'] as scalar_cache_global_hit,
       ProfileEvents['ScalarSubqueriesLocalCacheHit'] as scalar_cache_local_hit,
       ProfileEvents['ScalarSubqueriesCacheMiss'] as scalar_cache_miss
FROM system.query_log
WHERE
      current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%SELECT ''02177_CTE_GLOBAL_ON%'
  AND event_date >= yesterday() AND event_time > now() - interval 10 minute;

SELECT
    '02177_CTE_GLOBAL_OFF',
    ProfileEvents['SleepFunctionCalls'] as sleep_calls,
    ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds,
    ProfileEvents['ScalarSubqueriesGlobalCacheHit'] as scalar_cache_global_hit,
    ProfileEvents['ScalarSubqueriesLocalCacheHit'] as scalar_cache_local_hit,
    ProfileEvents['ScalarSubqueriesCacheMiss'] as scalar_cache_miss
FROM system.query_log
WHERE
      current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%02177_CTE_GLOBAL_OFF%'
  AND event_date >= yesterday() AND event_time > now() - interval 10 minute;

SELECT
    '02177_CTE_NEW_ANALYZER',
    ProfileEvents['SleepFunctionCalls'] as sleep_calls,
    ProfileEvents['SleepFunctionMicroseconds'] as sleep_microseconds,
    ProfileEvents['ScalarSubqueriesGlobalCacheHit'] as scalar_cache_global_hit,
    ProfileEvents['ScalarSubqueriesLocalCacheHit'] as scalar_cache_local_hit,
    ProfileEvents['ScalarSubqueriesCacheMiss'] as scalar_cache_miss
FROM system.query_log
WHERE
        current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%02177_CTE_NEW_ANALYZER%'
  AND event_date >= yesterday() AND event_time > now() - interval 10 minute;
