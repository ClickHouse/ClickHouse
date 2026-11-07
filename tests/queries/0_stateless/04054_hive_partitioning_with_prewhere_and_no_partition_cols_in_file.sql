-- Tags: no-parallel, no-fasttest, no-random-settings

-- Write: partition column stored only in the path, not in the Parquet file.
INSERT INTO FUNCTION s3(
    s3_conn,
    structure = 'business_date Date, ric String',
    filename = '04054_hive_prewhere',
    partition_strategy = 'hive',
    format = Parquet,
    partition_columns_in_data_file = 0
)
PARTITION BY business_date
FORMAT Values
(toDate('2026-02-03'), '.FOO'),
(toDate('2026-02-04'), '.BAR');

-- Verify that only the non-partition column (ric) is stored in the file.
SELECT num_columns
FROM s3(s3_conn, filename = '04054_hive_prewhere/**.parquet', format = ParquetMetadata)
LIMIT 1;

-- Read back with use_hive_partitioning=1 and a WHERE filter on both the hive partition column
-- (business_date, read from path) and a regular column (ric, read from file).
-- This triggers prewhere pushdown for the Parquet reader, which previously lost
-- hive_partition_columns_to_read_from_file_path and raised NOT_FOUND_COLUMN_IN_BLOCK.
SELECT business_date, ric
FROM s3(s3_conn, filename = '04054_hive_prewhere/business_date=*/*.parquet', format = Parquet)
WHERE business_date = '2026-02-03' AND ric = '.FOO'
SETTINGS use_hive_partitioning = 1, optimize_move_to_prewhere = 1;

-- Also verify that filtering on only the hive partition column works.
SELECT business_date, ric
FROM s3(s3_conn, filename = '04054_hive_prewhere/business_date=*/*.parquet', format = Parquet)
WHERE business_date = '2026-02-04'
SETTINGS use_hive_partitioning = 1, optimize_move_to_prewhere = 1;
