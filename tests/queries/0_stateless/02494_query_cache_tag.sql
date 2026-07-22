-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Store the result a single query with a tag in the query cache and check that the system table knows about the tag
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'abc';

SELECT query, tag FROM system.query_cache;

SELECT '---';

SYSTEM DROP QUERY CACHE;

-- Store the result of the same query with two different tags. The cache should store two entries.
SELECT 1 SETTINGS use_query_cache = true; -- default query_cache_tag = ''
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'abc';
SELECT query, tag FROM system.query_cache ORDER BY ALL;

SELECT '---';

SYSTEM DROP QUERY CACHE;

-- Like before but the tag is set standalone.

SET query_cache_tag = 'abc';
SELECT 1 SETTINGS use_query_cache = true;

SET query_cache_tag = 'def';
SELECT 1 SETTINGS use_query_cache = true;

SELECT query, tag FROM system.query_cache ORDER BY ALL;

SYSTEM DROP QUERY CACHE;
