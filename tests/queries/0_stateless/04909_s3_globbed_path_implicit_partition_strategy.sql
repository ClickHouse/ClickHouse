-- Tags: no-fasttest, no-random-settings
-- Tag no-fasttest: Depends on S3

-- Before 26.6, paths with ordinary globs were read-only and `PARTITION BY` was
-- ignored. Keep that behavior when no explicit `partition_strategy` is present.

SET compatibility = '26.6';
SET file_like_engine_default_partition_strategy = 'hive';

CREATE TABLE test_04909_glob_hive (d Date, x UInt64)
ENGINE = S3('s3://bucket/test_04909/**.parquet', 'Parquet')
PARTITION BY d;
SELECT 1;

-- This follows the metadata-load path used during server startup.
DETACH TABLE test_04909_glob_hive;
ATTACH TABLE test_04909_glob_hive;
SELECT 2;

SET compatibility = '26.4';
SET file_like_engine_default_partition_strategy = 'wildcard';

CREATE TABLE test_04909_glob_wildcard (d Date, x UInt64)
ENGINE = S3('s3://bucket/test_04909/**.parquet', 'Parquet')
PARTITION BY d;
SELECT 3;

-- A `wildcard` default must also keep a path without `{_partition_id}` at `NONE`.
CREATE TABLE test_04909_plain_wildcard (d Date, x UInt64)
ENGINE = S3('s3://bucket/test_04909/plain', 'Parquet')
PARTITION BY d;
SELECT 4;

SELECT count()
FROM system.tables
WHERE database = currentDatabase()
    AND name = 'test_04909_plain_wildcard'
    AND create_table_query LIKE '%partition_strategy = \'none\'%';

DETACH TABLE test_04909_plain_wildcard;
ATTACH TABLE test_04909_plain_wildcard;
SELECT empty(partition_key)
FROM system.tables
WHERE database = currentDatabase()
    AND name = 'test_04909_plain_wildcard';

-- An explicit `partition_strategy = 'none'` contradicts a `{_partition_id}` path:
-- only `wildcard` substitutes the placeholder, so the definition must be rejected.
CREATE TABLE test_04909_explicit_none (d Date, x UInt64)
ENGINE = S3('s3://bucket/test_04909/{_partition_id}.parquet', format = 'Parquet', partition_strategy = 'none')
PARTITION BY d; -- { serverError BAD_ARGUMENTS }

DROP TABLE test_04909_glob_hive;
DROP TABLE test_04909_glob_wildcard;
DROP TABLE test_04909_plain_wildcard;
