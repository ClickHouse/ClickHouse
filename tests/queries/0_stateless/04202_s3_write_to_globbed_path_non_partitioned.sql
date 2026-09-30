-- Tags: no-fasttest

-- Test: writing to a non-partitioned S3 path with globs throws DATABASE_ACCESS_DENIED.
-- Covers: src/Storages/ObjectStorage/StorageObjectStorageConfiguration.cpp:230-237
--   `hasGlobsIgnorePlaceholders` early-return branch when path has neither
--   `{_partition_id}` nor `{_schema_hash}` placeholder (returns `hasGlobs()` directly).
--
-- The PR's own test 03037 only covers the "with placeholder" branch
-- (`data_*_{_partition_id}.csv` with `partition by`).  This test covers the other
-- branch: a non-partitioned write to a path containing brace-expansion glob `{a,b}`.

insert into function s3('http://localhost:11111/test/04202_data_glob_brace_{a,b}.csv', 'CSV', 'x UInt32') select 1 as x; -- { serverError DATABASE_ACCESS_DENIED }
