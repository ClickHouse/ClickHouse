-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_analyzer = 1;

-- Tests the effect of setting 'query_condition_cache_store_conditions_as_plaintext'

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1_000_000); -- 1 mio rows sounds like a lot but the QCC doesn't cache anything if there is less data

SELECT '--- with move to PREWHERE';
SET optimize_move_to_prewhere = true;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Run two queries, with and without query_condition_cache_store_conditions_as_plaintext enabled';
SELECT count(*) FROM tab WHERE b = 10_000 SETTINGS use_query_condition_cache = true, query_condition_cache_store_conditions_as_plaintext = false FORMAT Null;
SELECT count(*) FROM tab WHERE b = 90_000 SETTINGS use_query_condition_cache = true, query_condition_cache_store_conditions_as_plaintext = true FORMAT Null;
SELECT part_name, condition FROM system.query_condition_cache ORDER BY condition;

SELECT '--- without move to PREWHERE';
SET optimize_move_to_prewhere = false;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Run two queries, with and without query_condition_cache_store_conditions_as_plaintext enabled';
SELECT count(*) FROM tab WHERE b = 10_000 SETTINGS use_query_condition_cache = true, query_condition_cache_store_conditions_as_plaintext = false FORMAT Null;
SELECT count(*) FROM tab WHERE b = 90_000 SETTINGS use_query_condition_cache = true, query_condition_cache_store_conditions_as_plaintext = true FORMAT Null;
SELECT part_name, condition FROM system.query_condition_cache ORDER BY condition;

SYSTEM DROP QUERY CONDITION CACHE;

DROP TABLE tab;
