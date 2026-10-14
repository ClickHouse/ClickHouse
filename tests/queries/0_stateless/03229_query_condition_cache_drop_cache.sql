-- Tags: no-parallel, no-release
-- Tag no-parallel: uses shared cache state and must remain isolated from concurrent cache tests.
-- Tag no-release: reads `table_uuid` from `system.query_condition_cache`, which is available only
-- in debug and sanitizer builds.
-- Tests that SYSTEM CLEAR QUERY CONDITION CACHE works

SET allow_experimental_analyzer = 1;

-- (it's silly to use what will be tested below but we have to assume other tests cluttered the query cache)

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number, number FROM numbers(1_000_000); -- 1 mio rows sounds like a lot but the QCC doesn't cache anything if there is less data

SELECT count(*) FROM tab WHERE b = 10_000 SETTINGS use_query_condition_cache = true FORMAT Null;

SELECT 'Expect a single entry in the cache';
SELECT count(*) FROM system.query_condition_cache WHERE table_uuid IN (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name IN ('tab'));

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT 'Expect empty cache after DROP CACHE';
SELECT count(*) FROM system.query_condition_cache WHERE table_uuid IN (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name IN ('tab'));

DROP TABLE tab;
