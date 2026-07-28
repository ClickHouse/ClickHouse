-- Tags: no-fasttest, no-random-settings
-- Tag no-fasttest: Depends on S3

SET compatibility = '26.6';
CREATE TABLE old_export (d Date, x UInt64)
ENGINE = S3('s3://bucket/export/data_{_partition_id}.parquet', 'Parquet')
PARTITION BY d; -- {serverError BAD_ARGUMENTS}

SET compatibility = '26.5';
CREATE TABLE old_export_compat_265 (d Date, x UInt64)
ENGINE = S3('s3://bucket/export/data_{_partition_id}.parquet', 'Parquet')
PARTITION BY d;
SELECT 1;

SET compatibility = '26.6';
SET file_like_engine_default_partition_strategy = 'wildcard';
CREATE TABLE old_export2 (d Date, x UInt64)
ENGINE = S3('s3://bucket/export/data_{_partition_id}.parquet', 'Parquet')
PARTITION BY d;
SELECT 1;

-- Backward compatibility: a pre-26.6 table with a `{_partition_id}` path (implicit wildcard)
-- must still load via ATTACH under the 26.6 `hive` default — the same code path the server
-- takes for every such table at startup and during upgrades. Before the fix this threw
-- `BAD_ARGUMENTS` and aborted server startup.
-- The explicit `hive` below is required: `SET compatibility` does not override the
-- explicitly-set `file_like_engine_default_partition_strategy = 'wildcard'` above, and the
-- ATTACH must run with the `hive` default in effect to be a real regression test.
SET compatibility = '26.6';
SET file_like_engine_default_partition_strategy = 'hive';
DETACH TABLE old_export_compat_265;
ATTACH TABLE old_export_compat_265;
SELECT 2;

DROP TABLE IF EXISTS old_export; -- never created: the first CREATE above is expected to throw
DROP TABLE old_export_compat_265;
DROP TABLE old_export2;

-- Mirror case: an implicit-`hive` table (created under the 26.6 default, no `{_partition_id}`
-- in the path) must still load via ATTACH when the effective default is `wildcard`
-- (e.g. a downgrade or a `compatibility = '26.5'` session). The strategy on load is derived
-- from the path shape, never from the mutable default.
SET compatibility = '26.6';
SET file_like_engine_default_partition_strategy = 'hive';
CREATE TABLE hive_export (d Date, x UInt64)
ENGINE = S3('s3://bucket/export2', 'Parquet')
PARTITION BY d;
SET file_like_engine_default_partition_strategy = 'wildcard';
DETACH TABLE hive_export;
ATTACH TABLE hive_export;
SELECT 3;
DROP TABLE hive_export;
