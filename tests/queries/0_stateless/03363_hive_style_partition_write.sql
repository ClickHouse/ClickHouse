-- Tags: no-parallel, no-fasttest, no-random-settings

DROP TABLE IF EXISTS t_03363_s3_sink, t_03363_s3_sink_read;

CREATE TABLE t_03363_s3_sink (year UInt16, country String, random UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_s3_sink_root', format = Parquet, partitioning_style='hive')
PARTITION BY (year, country);

INSERT INTO t_03363_s3_sink SETTINGS s3_truncate_on_insert=1 VALUES
    (2022, 'USA', 1),
    (2022, 'Canada', 2),
    (2023, 'USA', 3),
    (2023, 'Mexico', 4),
    (2024, 'France', 5),
    (2024, 'Germany', 6),
    (2024, 'Germany', 7),
    (1999, 'Brazil', 8),
    (2100, 'Japan', 9),
    (2024, '', 10),
    (2024, 'CN', 11);

-- while reading from object storage partitioned table is not supported, an auxiliary table must be created
CREATE TABLE t_03363_s3_sink_read (year UInt16, country String, random UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_s3_sink_root/**.parquet', format = Parquet);

select replaceRegexpAll(_path, '/[0-9]+\\.parquet', '/<snowflakeid>.parquet') AS _path, * from t_03363_s3_sink_read order by random SETTINGS use_hive_partitioning=1, input_format_parquet_use_native_reader=0;

DROP TABLE t_03363_s3_sink, t_03363_s3_sink_read;

CREATE TABLE t_03363_s3_sink (year UInt16, country String, random UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_s3_sink_root/{_partition_id}', format = Parquet, partitioning_style='hive')
PARTITION BY (year, country) -- {serverError BAD_ARGUMENTS};
