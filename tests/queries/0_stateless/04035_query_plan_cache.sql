-- Tags: no-parallel
-- Tag no-parallel: messes with internal caches and system.query_log

SET allow_experimental_query_plan_cache = 1;
SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

-- Record test start time to filter query_log entries from this run only
CREATE TEMPORARY TABLE test_start (ts DateTime) ENGINE = Memory;
INSERT INTO test_start VALUES (now());

DROP TABLE IF EXISTS t_plan_cache;
CREATE TABLE t_plan_cache (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_plan_cache VALUES (1, 'hello'), (2, 'world');

SYSTEM DROP QUERY PLAN CACHE;

-- Test 1: Basic hit/miss
SELECT a FROM t_plan_cache WHERE a = 1 SETTINGS log_comment = 'plan_cache_test1' FORMAT Null;
SELECT a FROM t_plan_cache WHERE a = 1 SETTINGS log_comment = 'plan_cache_test1' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 1: Basic hit/miss';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test1'
ORDER BY event_time_microseconds;

-- Test 2: SYSTEM CLEAR
SYSTEM DROP QUERY PLAN CACHE;
SELECT a FROM t_plan_cache WHERE a = 2 SETTINGS log_comment = 'plan_cache_test2' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 2: SYSTEM CLEAR';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test2'
ORDER BY event_time_microseconds;

-- Test 3: Schema change on non-replicated table (schema content hash detects the change)
SYSTEM DROP QUERY PLAN CACHE;
SELECT a FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test3' FORMAT Null;
ALTER TABLE t_plan_cache ADD COLUMN c UInt32 DEFAULT 0;
SELECT a FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test3' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 3: Schema change (non-replicated)';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test3'
ORDER BY event_time_microseconds;

-- Test 4: Non-deterministic exclusion
SYSTEM DROP QUERY PLAN CACHE;
SELECT now() SETTINGS log_comment = 'plan_cache_test4_now' FORMAT Null;
SELECT now() SETTINGS log_comment = 'plan_cache_test4_now' FORMAT Null;
SELECT rand() SETTINGS log_comment = 'plan_cache_test4_rand' FORMAT Null;
SELECT rand() SETTINGS log_comment = 'plan_cache_test4_rand' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 4: Non-deterministic exclusion';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment IN ('plan_cache_test4_now', 'plan_cache_test4_rand')
ORDER BY event_time_microseconds;

-- Test 5: System table exclusion
SYSTEM DROP QUERY PLAN CACHE;
SELECT * FROM system.one SETTINGS log_comment = 'plan_cache_test5' FORMAT Null;
SELECT * FROM system.one SETTINGS log_comment = 'plan_cache_test5' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 5: System table exclusion';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test5'
ORDER BY event_time_microseconds;

-- Test 6: Non-analyzer exclusion
SYSTEM DROP QUERY PLAN CACHE;
SET allow_experimental_analyzer = 0;
SELECT a FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test6' FORMAT Null;
SELECT a FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test6' FORMAT Null;
SET allow_experimental_analyzer = 1;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 6: Non-analyzer exclusion';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test6'
ORDER BY event_time_microseconds;

-- Test 7: Settings sensitivity
SYSTEM DROP QUERY PLAN CACHE;
SELECT a FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test7', max_block_size = 65505 FORMAT Null;
SELECT a FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test7', max_block_size = 1 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 7: Settings sensitivity';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test7'
ORDER BY event_time_microseconds;

-- Test 8: Explicit PREWHERE preserved on cache hit
SYSTEM DROP QUERY PLAN CACHE;
SELECT a FROM t_plan_cache PREWHERE a > 0 SETTINGS log_comment = 'plan_cache_test8' FORMAT Null;
SELECT a FROM t_plan_cache PREWHERE a > 0 SETTINGS log_comment = 'plan_cache_test8' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 8: Explicit PREWHERE preserved';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test8'
ORDER BY event_time_microseconds;

-- Test 9: Different aliases produce different cache entries
SYSTEM DROP QUERY PLAN CACHE;
SELECT a AS col1 FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test9' FORMAT Null;
SELECT a AS col2 FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test9' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 9: Alias differentiation';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test9'
ORDER BY event_time_microseconds;

-- Test 10: ALIAS column expression change invalidates cache
SYSTEM DROP QUERY PLAN CACHE;
ALTER TABLE t_plan_cache ADD COLUMN d UInt64 ALIAS a + 1;
SELECT d FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test10' FORMAT Null;
ALTER TABLE t_plan_cache MODIFY COLUMN d UInt64 ALIAS a + 2;
SELECT d FROM t_plan_cache SETTINGS log_comment = 'plan_cache_test10' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 10: ALIAS expression change invalidates cache';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test10'
ORDER BY event_time_microseconds;
ALTER TABLE t_plan_cache DROP COLUMN d;

-- Test 11: Subquery exclusion (queries with subqueries bypass cache entirely)
SYSTEM DROP QUERY PLAN CACHE;
DROP TABLE IF EXISTS t_plan_cache2;
CREATE TABLE t_plan_cache2 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_plan_cache2 VALUES (1);
SELECT a FROM t_plan_cache WHERE a IN (SELECT a FROM t_plan_cache2) SETTINGS log_comment = 'plan_cache_test11' FORMAT Null;
SELECT a FROM t_plan_cache WHERE a IN (SELECT a FROM t_plan_cache2) SETTINGS log_comment = 'plan_cache_test11' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 11: Subquery exclusion';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test11'
ORDER BY event_time_microseconds;
DROP TABLE t_plan_cache2;

-- Test 12: Quota reset to 0 means unlimited
-- Setting quota > 0, then resetting to 0 should allow unlimited cache usage.
SYSTEM DROP QUERY PLAN CACHE;
SELECT a FROM t_plan_cache SETTINGS query_plan_cache_size_in_bytes_quota = 1, log_comment = 'plan_cache_test13a' FORMAT Null;
SELECT a FROM t_plan_cache SETTINGS query_plan_cache_size_in_bytes_quota = 0, log_comment = 'plan_cache_test13b' FORMAT Null;
SELECT a FROM t_plan_cache SETTINGS query_plan_cache_size_in_bytes_quota = 0, log_comment = 'plan_cache_test13b' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 12: Quota reset to 0';
-- After resetting quota to 0 (unlimited), the second execution with quota=0 should be a cache hit
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test13b'
ORDER BY event_time_microseconds;

-- Test 13: `query_plan_optimize_prewhere` changes the cache key
SYSTEM DROP QUERY PLAN CACHE;
SELECT a FROM t_plan_cache WHERE a = 1 SETTINGS query_plan_optimize_prewhere = 0, log_comment = 'plan_cache_test14' FORMAT Null;
SELECT a FROM t_plan_cache WHERE a = 1 SETTINGS query_plan_optimize_prewhere = 1, log_comment = 'plan_cache_test14' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 13: query_plan_optimize_prewhere sensitivity';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'plan_cache_test14'
ORDER BY event_time_microseconds;

-- Test 14: `log_comment` does not affect the cache key
SYSTEM DROP QUERY PLAN CACHE;
SELECT a FROM t_plan_cache WHERE a = 1 SETTINGS log_comment = 'plan_cache_test15a' FORMAT Null;
SELECT a FROM t_plan_cache WHERE a = 1 SETTINGS log_comment = 'plan_cache_test15b' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 14: log_comment ignored in cache key';
SELECT ProfileEvents['QueryPlanCacheHits'] AS hits, ProfileEvents['QueryPlanCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= (SELECT ts FROM test_start)
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment IN ('plan_cache_test15a', 'plan_cache_test15b')
ORDER BY event_time_microseconds;

DROP TABLE t_plan_cache;
