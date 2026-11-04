-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Suppress expected warnings about the corrupted snapshot summary.
SET send_logs_level = 'fatal';

-- The fixture is a copy of the `iceberg_malformed_manifest_row_counts_test` table (1 row,
-- snapshot summary corrupted to claim `total-records = 100`, manifest list with a
-- malformed `added_rows_count = -1`) whose manifest file additionally carries a malformed
-- `record_count = -1` for its data file. With both metadata row-count sources malformed,
-- the trivial count must fail closed to a real scan: it must not throw, must not trust
-- the summary (100), and must not sum the negative record count (which would come out as
-- 18446744073709551615 after the conversion to an unsigned row count).

-- Pinned because the test runner randomizes the setting.
SELECT count() FROM icebergS3(s3_conn, filename='iceberg_negative_record_count_test') SETTINGS optimize_trivial_count_query = 1;

-- Sanity check: same result as a full scan.
SELECT count() FROM icebergS3(s3_conn, filename='iceberg_negative_record_count_test') SETTINGS optimize_trivial_count_query = 0;

-- No exact metadata count exists, so the optimization must not be applied.
SELECT count() FROM (EXPLAIN SELECT count() FROM icebergS3(s3_conn, filename='iceberg_negative_record_count_test') SETTINGS optimize_trivial_count_query = 1) WHERE explain LIKE '%Optimized trivial count%';
