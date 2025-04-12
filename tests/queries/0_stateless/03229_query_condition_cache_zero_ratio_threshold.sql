-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_analyzer = 1;

-- Tests the effect of setting 'query_condition_cache_zero_ratio_threshold'

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1_000_000); -- 1 mio rows sounds like a lot but the QCC doesn't cache anything if there is less data

SELECT '--- with move to PREWHERE';
SET optimize_move_to_prewhere = true;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Expect one entry in the cache after the first query';
SELECT count(*) FROM tab WHERE b < 500_000 SETTINGS use_query_condition_cache = true, query_condition_cache_zero_ratio_threshold = 0.2;
SELECT count(*) FROM system.query_condition_cache;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Expect no entry in the cache';
SELECT count(*) FROM tab WHERE b < 500_000 SETTINGS use_query_condition_cache = true, query_condition_cache_zero_ratio_threshold = 0.8;
SELECT count(*) FROM system.query_condition_cache;

SELECT '--- without move to PREWHERE';
SET optimize_move_to_prewhere = false;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Expect one entry in the cache after the first query';
SELECT count(*) FROM tab WHERE b < 500_000 SETTINGS use_query_condition_cache = true, query_condition_cache_zero_ratio_threshold = 0.2;
SELECT count(*) FROM system.query_condition_cache;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Expect no entry in the cache';
SELECT count(*) FROM tab WHERE b < 500_000 SETTINGS use_query_condition_cache = true, query_condition_cache_zero_ratio_threshold = 0.8;
SELECT count(*) FROM system.query_condition_cache;

SYSTEM DROP QUERY CONDITION CACHE;

DROP TABLE tab;
