-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS tab;

SELECT '--- with move to PREWHERE';

SYSTEM DROP QUERY CONDITION CACHE;

CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1000000);

SELECT 'Expect the cache to be initially empty.';
SELECT count(*) FROM system.query_condition_cache;

SELECT 'Expect that the cache contains one entry after the first query.';
SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT 'If the same query runs again, the cache still contains just a single entry.';
SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT '--- without move to PREWHERE';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Expect the cache to be initially empty.';
SELECT count(*) FROM system.query_condition_cache;

SELECT 'Expect that the cache contains one entry after the first query.';
SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT 'If the same query runs again, the cache still contains just a single entry.';
SELECT count(*) FROM tab WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

DROP TABLE tab;
