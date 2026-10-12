-- This test genuinely exercises DROP-by-tag semantics for several distinct tags, so the tags used
-- by the queries under test are prefixed with this file's own name to keep them from colliding
-- with tags used by other, concurrently running test files. `system.query_cache` is filtered to
-- those prefixed tags so that entries left behind by unrelated tests are not counted.
-- (Query parameters such as {CLICKHOUSE_TEST_UNIQUE_NAME:String} cannot be used here: they are not
-- accepted as the value of a `SETTINGS`/`SET` clause or of `SYSTEM ... TAG`, only in expression
-- positions such as a `WHERE` clause.)

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_drop_cache';

SELECT 'Cache query result in query cache';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = '02494_query_cache_drop_cache';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_drop_cache') AS test_query_cache SETTINGS use_query_cache = 0;

SELECT 'DROP entries with a certain tag, no entry will match';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_drop_cache_tag';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_drop_cache') AS test_query_cache SETTINGS use_query_cache = 0;

SELECT 'After a full DROP, the cache is empty now';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_drop_cache';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_drop_cache') AS test_query_cache SETTINGS use_query_cache = 0;

-- More tests for DROP with tags:

SELECT 'Cache query result with different or no tag in query cache';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = '02494_query_cache_drop_cache';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = '02494_query_cache_drop_cache_abc';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = '02494_query_cache_drop_cache_def';
SELECT 2 SETTINGS use_query_cache = true, query_cache_tag = '02494_query_cache_drop_cache';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag LIKE '02494_query_cache_drop_cache%') AS test_query_cache SETTINGS use_query_cache = 0;

SELECT 'DROP entries with certain tags';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_drop_cache';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag LIKE '02494_query_cache_drop_cache%') AS test_query_cache SETTINGS use_query_cache = 0;
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_drop_cache_def';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag LIKE '02494_query_cache_drop_cache%') AS test_query_cache SETTINGS use_query_cache = 0;
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_drop_cache_abc';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag LIKE '02494_query_cache_drop_cache%') AS test_query_cache SETTINGS use_query_cache = 0;
