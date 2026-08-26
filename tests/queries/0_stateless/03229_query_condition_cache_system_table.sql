-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests system table 'system.query_condition_cache'

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1_000_000); -- 1 mio rows sounds like a lot but the QCC doesn't cache anything if there is less data

SELECT '--- with move to PREWHERE';
SET optimize_move_to_prewhere = true;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Expect one entry in the cache after the first query.';
SELECT count(*) FROM tab WHERE b = 10_000 SETTINGS use_query_condition_cache = true FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT 'If the same query runs again, the cache still contains just a single entry.';
SELECT count(*) FROM tab WHERE b = 10_000 SETTINGS use_query_condition_cache = true FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT '--- without move to PREWHERE';
SET optimize_move_to_prewhere = false;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Expect one entry in the cache after the first query.';
SELECT count(*) FROM tab WHERE b = 10_000 SETTINGS use_query_condition_cache = true FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT 'If the same query runs again, the cache still contains just a single entry.';
SELECT count(*) FROM tab WHERE b = 10_000 SETTINGS use_query_condition_cache = true FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

DROP TABLE tab;
