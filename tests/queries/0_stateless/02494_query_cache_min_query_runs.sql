-- Tags: no-parallel
-- Tag no-parallel: counts entries under a fixed `query_cache_tag`; concurrent copies of this test (flaky and targeted checks) clear and fill the same tag, so the count is off.

SET query_cache_tag = '02494_query_cache_min_query_runs';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_min_query_runs';

-- Cache the query after the 1st query invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 0;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_min_query_runs') AS test_query_cache;

SELECT '---';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_min_query_runs';

-- Cache the query result after the 2nd query invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 1;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_min_query_runs') AS test_query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 1;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_min_query_runs') AS test_query_cache;

SELECT '---';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_min_query_runs';

-- Cache the query result after the 3rd query invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_min_query_runs') AS test_query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_min_query_runs') AS test_query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_min_query_runs') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_min_query_runs';
