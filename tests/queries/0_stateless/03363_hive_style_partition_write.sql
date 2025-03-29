-- Tags: no-parallel, no-fasttest, no-random-settings

DROP TABLE IF EXISTS t_03363_parquet, t_03363_parquet_read, t_03363_csv, t_03363_csv_read;

CREATE TABLE t_03363_parquet (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet', format = Parquet, partitioning_style='hive')
PARTITION BY (year, country);

INSERT INTO t_03363_parquet SETTINGS s3_truncate_on_insert=1 VALUES
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
CREATE TABLE t_03363_parquet_read (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet/**.parquet', format = Parquet);

-- distinct because minio isn't cleaned up
select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.parquet', '/<snowflakeid>.parquet') AS _path, counter from t_03363_parquet_read order by counter SETTINGS use_hive_partitioning=1, input_format_parquet_use_native_reader=0;

-- CSV test
CREATE TABLE t_03363_csv (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_csv', format = CSV, partitioning_style='hive')
PARTITION BY (year, country);

INSERT INTO t_03363_csv SETTINGS s3_truncate_on_insert=1 VALUES
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

CREATE TABLE t_03363_csv_read (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_csv/**.csv', format = CSV);

select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.csv', '/<snowflakeid>.csv') AS _path, counter from t_03363_csv_read order by counter SETTINGS use_hive_partitioning=1;

-- s3 table function
INSERT INTO FUNCTION s3(s3_conn, filename='t_03363_function', format=Parquet, partitioning_style='hive') PARTITION BY (year, country) SELECT * FROM t_03363_parquet_read;
select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.parquet', '/<snowflakeid>.parquet') AS _path, counter from s3(s3_conn, filename='t_03363_function/**.parquet') order by counter SETTINGS use_hive_partitioning=1;

-- hive with partition id placeholder
CREATE TABLE t_03363_s3_sink (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet/{_partition_id}', format = Parquet, partitioning_style='hive')
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS};

-- unknown partitioning style
CREATE TABLE t_03363_s3_sink (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet', format = Parquet, partitioning_style='abc')
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS};

-- auto partitioning style without partition_id wildcard
CREATE TABLE t_03363_s3_sink (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet', format = Parquet)
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS};

DROP TABLE IF EXISTS t_03363_parquet, t_03363_parquet_read, t_03363_csv, t_03363_csv_read;
