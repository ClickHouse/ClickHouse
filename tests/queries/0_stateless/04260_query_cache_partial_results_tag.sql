-- Tags: no-parallel
-- Tag no-parallel: Messes with the query result cache

SET query_cache_partial_results = 1;
SET query_cache_min_query_runs = 0;
SET query_cache_min_query_duration = 0;
SET query_cache_ttl = 600;

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_partial_tag;
CREATE TABLE t_partial_tag (key UInt64, value String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_partial_tag SELECT number, toString(number) FROM numbers(100);

SELECT '--- Populate cache with tag alpha ---';
SELECT key, value FROM t_partial_tag WHERE key < 10 ORDER BY key
    SETTINGS use_query_cache = 1, query_cache_tag = 'alpha';

SELECT '--- Cache entries after tag alpha ---';
SELECT count() > 0 FROM system.query_cache;

SELECT '--- Same query with tag beta creates separate entry ---';
SELECT key, value FROM t_partial_tag WHERE key < 10 ORDER BY key
    SETTINGS use_query_cache = 1, query_cache_tag = 'beta';

SELECT '--- DROP TAG alpha removes only alpha entries ---';
SYSTEM DROP QUERY CACHE TAG 'alpha';
SELECT count() > 0 FROM system.query_cache;

SELECT '--- DROP TAG beta removes remaining entries ---';
SYSTEM DROP QUERY CACHE TAG 'beta';
SELECT count() FROM system.query_cache;

SELECT '--- Mutual exclusion: both flags reject early ---';
SELECT 1 SETTINGS use_query_cache = 1, query_cache_before_limit_and_order_by = 1, query_cache_partial_results = 1; -- { serverError BAD_ARGUMENTS }

SELECT '--- Correctness check ---';
SYSTEM DROP QUERY CACHE;
SET query_cache_partial_results = 0;
SELECT key, value FROM t_partial_tag WHERE key < 10 ORDER BY key;

DROP TABLE t_partial_tag;

SYSTEM DROP QUERY CACHE;
