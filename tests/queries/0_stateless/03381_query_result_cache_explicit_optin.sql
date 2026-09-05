
SET query_cache_tag = '03381_query_result_cache_explicit_optin';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_explicit_optin';

-- Test 1: Explicit opt-in on subquery creates cache entry with is_subquery = 1
SELECT * FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true) ORDER BY number;

SELECT count(*) > 0 FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_explicit_optin') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1 (true)

-- Test 2: Repeated runs hit the cache at least once (verified via ProfileEvents)
SELECT * FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true) ORDER BY number;
SELECT * FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true) ORDER BY number;
SELECT * FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true) ORDER BY number;

SYSTEM FLUSH LOGS query_log;

-- The query cache is process-wide and size-limited, so another test can evict this entry
-- between two runs. Assert that at least one of the repeated runs was a cache hit.
SELECT countIf(ProfileEvents['QueryCacheHits'] > 0) > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT number FROM numbers(5) SETTINGS use_query_cache = true%'
  AND query NOT LIKE '%system.query_log%';

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_explicit_optin';
