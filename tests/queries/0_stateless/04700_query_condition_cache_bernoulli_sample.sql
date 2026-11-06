-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the query condition cache is populated per replica, so the poisoning is
--   not reliably reproducible with parallel replicas

-- Bernoulli sampling (SAMPLE on a table without a SAMPLE BY key) drops rows inside the reader
-- before the WHERE FilterTransform runs, so a mark could be cached as non-matching after only
-- its sampled subset was inspected. Like native SAMPLE, such reads must not write to the query
-- condition cache, or a later non-SAMPLE query with the same predicate would skip marks and
-- return too few rows.

SET allow_experimental_bernoulli_sample = 1;
SET bernoulli_sample_seed = 42;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, val String)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

-- 10000 rows at `index_granularity = 2` are 5000 marks: enough for the sampled read to leave
-- zero-matching chunks behind, and cheap enough for the sanitizer and cloud test runs.
INSERT INTO tab SELECT number, 'hit' FROM numbers(10000);

SELECT '--- WHERE with a selective predicate';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'A Bernoulli SAMPLEing query must NOT write to the query condition cache.';
-- max_block_size = 2 makes chunks align with single marks, so a mark where the sampling kept only
-- the odd row yields a zero-matching chunk for that mark — the exact shape that would poison the cache.
SELECT count() FROM tab SAMPLE 0.1 WHERE id % 2 = 0 SETTINGS use_query_condition_cache = true, max_block_size = 2 FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT 'A non-SAMPLEing query must return the entire matching row set.';
SELECT count() FROM tab WHERE id % 2 = 0 SETTINGS use_query_condition_cache = true;

DROP TABLE tab;
