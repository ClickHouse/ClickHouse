-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that SYSTEM DROP QUERY CONDITION CACHE works

SET allow_experimental_analyzer = 1;

-- (it's silly to use what will be tested below but we have to assume other tests cluttered the query cache)
SYSTEM DROP QUERY CONDITION CACHE;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1_000_000); -- 1 mio rows sounds like a lot but the QCC doesn't cache anything if there is less data

SELECT count(*) FROM tab WHERE b = 10_000 SETTINGS use_query_condition_cache = true FORMAT Null;

SELECT 'Expect a single entry in the cache';
SELECT count(*) FROM system.query_condition_cache;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'Expect empty cache after DROP CACHE';
SELECT count(*) FROM system.query_condition_cache;

DROP TABLE tab;
