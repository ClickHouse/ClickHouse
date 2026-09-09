-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- The fixture is a copy of the `est` table (1 row) whose snapshot summary was edited
-- to claim `total-records = 100`. Writers maintain that hint as parent total + added,
-- so it can silently diverge from the data when the table history contains a corrupted
-- commit. count() must not report 100: the summary hint is never used as a data source,
-- and the disagreement with the manifest files' `record_count` disables the metadata-only
-- count altogether, because it does not tell which of the two sources is corrupted.

-- The corrupted summary makes the server log an expected warning; keep it out of stderr.
SET send_logs_level = 'fatal';

-- Trivial count enabled (pinned: the test runner randomizes this setting): must return
-- the real row count, not 100.
SELECT count() FROM icebergS3(s3_conn, filename='iceberg_corrupted_summary_test') SETTINGS optimize_trivial_count_query = 1;

-- Sanity check: same result as a full scan.
SELECT count() FROM icebergS3(s3_conn, filename='iceberg_corrupted_summary_test') SETTINGS optimize_trivial_count_query = 0;

-- The metadata is self-contradictory, so no metadata-only count is exact and the
-- optimization must not be applied: the count above comes from reading the data files.
SELECT count() FROM (EXPLAIN SELECT count() FROM icebergS3(s3_conn, filename='iceberg_corrupted_summary_test') SETTINGS optimize_trivial_count_query = 1) WHERE explain LIKE '%Optimized trivial count%';
