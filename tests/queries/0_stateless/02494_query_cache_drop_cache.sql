-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- (it's silly to use what will be tested below but we have to assume other tests cluttered the query cache)
SYSTEM DROP QUERY CACHE;

SELECT 'Cache query result in query cache';
SELECT 1 SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache;

SELECT 'DROP entries with a certain tag, no entry will match';
SYSTEM DROP QUERY CACHE TAG 'tag';
SELECT count(*) FROM system.query_cache;

SELECT 'After a full DROP, the cache is empty now';
SYSTEM DROP QUERY CACHE;
SELECT count(*) FROM system.query_cache;

-- More tests for DROP with tags:

SELECT 'Cache query result with different or no tag in query cache';
SELECT 1 SETTINGS use_query_cache = true;
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'abc';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'def';
SELECT 2 SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache;

SELECT 'DROP entries with certain tags';
SYSTEM DROP QUERY CACHE TAG '';
SELECT count(*) FROM system.query_cache;
SYSTEM DROP QUERY CACHE TAG 'def';
SELECT count(*) FROM system.query_cache;
SYSTEM DROP QUERY CACHE TAG 'abc';
SELECT count(*) FROM system.query_cache;
