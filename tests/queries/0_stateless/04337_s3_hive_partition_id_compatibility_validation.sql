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

SET file_like_engine_default_partition_strategy = 'wildcard';
CREATE TABLE old_export2 (d Date, x UInt64)
ENGINE = S3('s3://bucket/export/data_{_partition_id}.parquet', 'Parquet')
PARTITION BY d;
SELECT 1;