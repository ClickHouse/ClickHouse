-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

-- Regression test: the count-only shortcut (`optimize_count_from_files`) and the count-from-cache
-- shortcut (`use_cache_for_count_from_files`) must be disabled when a row-level filter (row policy)
-- or storage `PREWHERE` applies, otherwise `SELECT count()` returns the unfiltered file row count.

SET optimize_count_from_files = 1;
SET use_cache_for_count_from_files = 1;
SET optimize_trivial_count_query = 1;
SET input_format_parquet_use_native_reader_v3 = 1;

DROP TABLE IF EXISTS test_count_policy;

CREATE TABLE test_count_policy (x UInt64) ENGINE = File(Parquet);
INSERT INTO test_count_policy SELECT number FROM numbers(10);

-- Unfiltered count; also primes the count cache.
SELECT count() FROM test_count_policy;

-- `PREWHERE` must be applied to the count, with and without the count cache.
SELECT count() FROM test_count_policy PREWHERE x >= 5;
SELECT count() FROM test_count_policy PREWHERE x >= 5 SETTINGS use_cache_for_count_from_files = 0;

-- A row policy must be applied to the count, with and without the count cache.
CREATE ROW POLICY 04618_row_policy ON test_count_policy FOR SELECT USING x < 3 TO ALL;

SELECT count() FROM test_count_policy;
SELECT count() FROM test_count_policy SETTINGS use_cache_for_count_from_files = 0;
SELECT count() FROM test_count_policy PREWHERE x >= 1;

DROP ROW POLICY 04618_row_policy ON test_count_policy;

-- Back to the unfiltered count after the policy is dropped.
SELECT count() FROM test_count_policy;

DROP TABLE test_count_policy;
