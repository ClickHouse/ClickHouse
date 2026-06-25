-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the query condition cache is populated per replica, so the poisoning is
--   not reliably reproducible with parallel replicas

-- Tests that a sampled query does not poison the query condition cache for a later non-sampled query.
-- The cache key encodes only the WHERE/PREWHERE predicate, not the SAMPLE clause, so a sampling-narrowed
-- mark mask must never be written (issue: SAMPLE facet of the #104203 query-condition-cache family).

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (sk UInt64, id UInt64, v String)
ENGINE = MergeTree ORDER BY (sk, id) SAMPLE BY sk
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- 12 separate parts, each with several marks; the sampling key varies so SAMPLE prunes whole marks.
SYSTEM STOP MERGES tab;
INSERT INTO tab SELECT sipHash64(number * 100 + 1), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 2), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 3), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 4), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 5), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 6), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 7), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 8), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 9), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 10), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 11), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 12), number, 'hit' FROM numbers(100000);

-- WHERE on a non-key column: predicate is moved to PREWHERE (runtime write path).
SELECT '--- WHERE on non-key column (PREWHERE write path)';
SET optimize_move_to_prewhere = true;

SYSTEM DROP QUERY CONDITION CACHE;
SELECT 'A sampled query must not write to the query condition cache.';
SELECT count() FROM tab SAMPLE 0.1 WHERE v = 'hit' SETTINGS use_query_condition_cache = true FORMAT Null;
SELECT count() FROM system.query_condition_cache;
SELECT 'A later non-sampled query returns the full count (not poisoned).';
SELECT count() FROM tab WHERE v = 'hit' SETTINGS use_query_condition_cache = true;

-- WHERE on a key-prefix column: predicate is resolved by index analysis (analysis write path).
SELECT '--- WHERE on key column (index-analysis write path)';

SYSTEM DROP QUERY CONDITION CACHE;
SELECT 'A sampled query must not write to the query condition cache.';
SELECT count() FROM tab SAMPLE 0.1 WHERE id = 42 SETTINGS use_query_condition_cache = true FORMAT Null;
SELECT count() FROM system.query_condition_cache;
SELECT 'A later non-sampled query returns the full count (not poisoned).';
SELECT count() FROM tab WHERE id = 42 SETTINGS use_query_condition_cache = true;

DROP TABLE tab;
