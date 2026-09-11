
SET query_cache_tag = '02494_query_cache_min_query_duration';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_min_query_duration';

-- This creates an entry in the query cache ...
SELECT 1 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_min_query_duration') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_min_query_duration';

SELECT '---';

-- ... but this does not because the query executes much faster than the specified minumum query duration for caching the result
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_duration = 10000;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_min_query_duration') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_min_query_duration';
