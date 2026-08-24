-- Tags: no-parallel, no-fasttest, no-random-settings

INSERT INTO FUNCTION s3(
    s3_conn,
    filename='03631',
    format=Parquet,
    partition_strategy='hive',
    partition_columns_in_data_file=1) PARTITION BY (year, country) SELECT 'Brazil' as country, 2025 as year, 1 as id;

-- distinct because minio isn't cleaned up
SELECT count(distinct year) FROM s3(s3_conn, filename='03631/**.parquet', format=RawBLOB) SETTINGS use_hive_partitioning=1;

DESCRIBE s3(s3_conn, filename='03631/**.parquet', format=RawBLOB) SETTINGS use_hive_partitioning=1;
