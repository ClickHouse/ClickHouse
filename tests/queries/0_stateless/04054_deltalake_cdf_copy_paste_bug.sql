-- Tags: no-fasttest
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100449
--
-- Copy-paste bug: both OR-branches of is_delta_lake_cdf checked
-- delta_lake_snapshot_start_version.  Setting only delta_lake_snapshot_end_version
-- must also be detected as a CDF request.
--
-- The constructor check in StorageObjectStorage fires before any network access,
-- so a non-existent URL is sufficient to trigger it.

SET delta_lake_snapshot_end_version = 1;
CREATE TABLE t_cdf_bug (x UInt32) ENGINE = S3('http://nonexistent_host/bucket/file.parquet', 'Parquet'); -- { serverError BAD_ARGUMENTS }
