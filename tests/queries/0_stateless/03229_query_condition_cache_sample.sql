-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the query condition cache is populated per replica, so the poisoning is
--   not reliably reproducible with parallel replicas

-- Bug #104203: SAMPLE clauses in SELECTs confuse ("poison") the query condition cache.
-- This was resolved by not writing in the query condition cache for SELECTs with SAMPLE clause.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (sample_key UInt64, id UInt64, val String)
ENGINE = MergeTree
ORDER BY (sample_key, id)
SAMPLE BY sample_key
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- Insert 6 parts. The sampling key varies so SAMPLE prunes whole marks.
SYSTEM STOP MERGES tab;
INSERT INTO tab SELECT sipHash64(number * 100 + 1), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 2), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 3), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 4), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 5), number, 'hit' FROM numbers(100000);
INSERT INTO tab SELECT sipHash64(number * 100 + 6), number, 'hit' FROM numbers(100000);

SELECT '--- WHERE on non-primary-key column';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'A SAMPLEing query must NOT write to the query condition cache.';
SELECT count() FROM tab SAMPLE 0.1 WHERE val = 'hit' FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT 'A non-SAMPLEing query must returns the entire table content.';
SELECT count() FROM tab WHERE val = 'hit' SETTINGS use_query_condition_cache = true;

SELECT '--- WHERE on primary key column';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'A SAMPLEing query must NOT write to the query condition cache.';
SELECT count() FROM tab SAMPLE 0.1 WHERE id = 42 FORMAT Null;
SELECT count() FROM system.query_condition_cache;
SELECT 'A non-SAMPLEing query must returns the entire table content.';
SELECT count() FROM tab WHERE id = 42;

DROP TABLE tab;
