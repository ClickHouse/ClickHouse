-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SET send_logs_level = 'fatal';

-- The fixture is a 1-row table whose snapshot summary has no `total-records`, whose manifest
-- list has a malformed `added_rows_count = -1`, and whose manifest file carries a malformed
-- `record_count = -1` for its data file. Without a summary row count, the trivial count is
-- derived from the manifest files, and the negative record count must make it fail closed to
-- a real scan: it must not throw and must not sum the negative record count (which would come
-- out as 18446744073709551615 after the conversion to an unsigned row count).

-- Pinned because the test runner randomizes the setting.
SELECT count() FROM icebergS3(s3_conn, filename='iceberg_negative_record_count_test') SETTINGS optimize_trivial_count_query = 1;

-- Sanity check: same result as a full scan.
SELECT count() FROM icebergS3(s3_conn, filename='iceberg_negative_record_count_test') SETTINGS optimize_trivial_count_query = 0;

-- No exact metadata count exists, so the optimization must not be applied.
SELECT count() FROM (EXPLAIN SELECT count() FROM icebergS3(s3_conn, filename='iceberg_negative_record_count_test') SETTINGS optimize_trivial_count_query = 1) WHERE explain LIKE '%Optimized trivial count%';
