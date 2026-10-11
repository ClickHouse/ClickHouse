-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- A cached subquery result must be complete even if the outer query reads only a part of it.
-- https://github.com/ClickHouse/ClickHouse/issues/124409

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- The outer LIMIT must not be pushed down below the step which stores the subquery result in the cache.
SELECT number FROM (SELECT number FROM numbers(100000) SETTINGS use_query_cache = 1) LIMIT 3 FORMAT Null;
SELECT count() FROM (SELECT number FROM numbers(100000) SETTINGS use_query_cache = 1);
SELECT count() FROM system.query_cache WHERE is_subquery = 1;

SYSTEM DROP QUERY CACHE;

-- DISTINCT with a limit hint stops reading the subquery early. Such a partial result must not be cached.
SELECT DISTINCT number % 3 FROM (SELECT number FROM numbers(200000) SETTINGS use_query_cache = 1, max_block_size = 1000) LIMIT 2 FORMAT Null;
SELECT count() FROM system.query_cache WHERE is_subquery = 1;
SELECT count() FROM (SELECT number FROM numbers(200000) SETTINGS use_query_cache = 1, max_block_size = 1000);

SYSTEM DROP QUERY CACHE;
