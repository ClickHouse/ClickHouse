 -- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that setting 'query_condition_cache_selectivity_threshold' works.

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1_000_000); -- 1 mio rows sounds like a lot but the QCC doesn't cache anything if there is less data

OPTIMIZE TABLE tab FORCE; -- force a single part to avoid flaky results

SELECT '--- with move to PREWHERE';
SET optimize_move_to_prewhere = true;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT count(*) FROM tab WHERE b < 500_000 SETTINGS use_query_condition_cache = true, query_condition_cache_selectivity_threshold = 0.2 FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT count(*) FROM tab WHERE b < 500_000 SETTINGS use_query_condition_cache = true, query_condition_cache_selectivity_threshold = 0.8 FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT '--- without move to PREWHERE';
SET optimize_move_to_prewhere = false;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT count(*) FROM tab WHERE b < 500_000 SETTINGS use_query_condition_cache = true, query_condition_cache_selectivity_threshold = 0.2 FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

SELECT count(*) FROM tab WHERE b < 500_000 SETTINGS use_query_condition_cache = true, query_condition_cache_selectivity_threshold = 0.8 FORMAT Null;
SELECT count(*) FROM system.query_condition_cache;

DROP TABLE tab;
