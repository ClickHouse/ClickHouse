-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Suppress expected warnings about the corrupted snapshot summary.
SET send_logs_level = 'fatal';

-- The fixture is a copy of the `iceberg_corrupted_summary_test` table (1 row, snapshot
-- summary corrupted to claim `total-records = 100`) whose manifest list additionally has
-- a malformed `added_rows_count = -1` (the field is required non-negative in format v2).
-- Malformed manifest-list row counts must not make the table unreadable and must not
-- reinstate trust in the summary hint: totalRows() derives the count by scanning the
-- manifest files, whose file-level `record_count` gives the exact answer.

-- Trivial count enabled (pinned: the test runner randomizes this setting): must return
-- the real row count via the manifest-file scan, not 100 from the summary.
SELECT count() FROM icebergS3(s3_conn, filename='iceberg_malformed_manifest_row_counts_test') SETTINGS optimize_trivial_count_query = 1;

-- Sanity check: same result as a full scan.
SELECT count() FROM icebergS3(s3_conn, filename='iceberg_malformed_manifest_row_counts_test') SETTINGS optimize_trivial_count_query = 0;

-- The optimization must still be applied (the manifest-file scan is a metadata-only path).
SELECT count() FROM (EXPLAIN SELECT count() FROM icebergS3(s3_conn, filename='iceberg_malformed_manifest_row_counts_test') SETTINGS optimize_trivial_count_query = 1) WHERE explain LIKE '%Optimized trivial count%';
