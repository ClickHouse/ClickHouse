-- Tags: no-parallel, no-fasttest, no-random-settings

DROP TABLE IF EXISTS t_03363_parquet, t_03363_csv;

CREATE TABLE t_03363_parquet (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet', format = Parquet, partition_strategy='hive')
PARTITION BY (year, country);

INSERT INTO t_03363_parquet VALUES
    (2022, 'USA', 1),
    (2022, 'Canada', 2),
    (2023, 'USA', 3),
    (2023, 'Mexico', 4),
    (2024, 'France', 5),
    (2024, 'Germany', 6),
    (2024, 'Germany', 7),
    (1999, 'Brazil', 8),
    (2100, 'Japan', 9),
    (2024, 'CN', 10),
    (2025, '', 11);

-- distinct because minio isn't cleaned up
select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.parquet', '/<snowflakeid>.parquet') AS _path, counter from t_03363_parquet order by counter;

-- CSV test
CREATE TABLE t_03363_csv (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_csv', format = CSV, partition_strategy='hive')
PARTITION BY (year, country);

INSERT INTO t_03363_csv VALUES
    (2022, 'USA', 1),
    (2022, 'Canada', 2),
    (2023, 'USA', 3),
    (2023, 'Mexico', 4),
    (2024, 'France', 5),
    (2024, 'Germany', 6),
    (2024, 'Germany', 7),
    (1999, 'Brazil', 8),
    (2100, 'Japan', 9),
    (2024, 'CN', 10),
    (2025, '', 11);

select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.csv', '/<snowflakeid>.csv') AS _path, counter from t_03363_csv order by counter;

-- s3 table function
INSERT INTO FUNCTION s3(s3_conn, filename='t_03363_function', format=Parquet, partition_strategy='hive') PARTITION BY (year, country) SELECT country, year, counter FROM t_03363_parquet;
select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.parquet', '/<snowflakeid>.parquet') AS _path, counter from s3(s3_conn, filename='t_03363_function/**.parquet') order by counter;

-- should output 1 because partition columns are not written down to the file by default when hive style is being used
select num_columns from s3(s3_conn, filename='t_03363_function/**.parquet', format=ParquetMetadata) limit 1;

INSERT INTO FUNCTION s3(s3_conn, filename='t_03363_function_write_down_partition_columns', format=Parquet, partition_strategy='hive', partition_columns_in_data_file=1) PARTITION BY (year, country) SELECT country, year, counter FROM t_03363_parquet;
select num_columns from s3(s3_conn, filename='t_03363_function_write_down_partition_columns/**.parquet', format=ParquetMetadata) limit 1;

-- hive partitioning = 0 so we know it is not reading columns from the path
select * from s3(s3_conn, filename='t_03363_function_write_down_partition_columns/**.parquet', format=Parquet) order by counter limit 1 SETTINGS use_hive_partitioning=0;

-- only partition columns
INSERT INTO FUNCTION s3(s3_conn, filename='t_03363_parquet', format=Parquet, partition_strategy='hive') PARTITION BY (year, country) SELECT 2020 as year, 'Brazil' as country; -- {serverError INCORRECT_DATA};

-- hive with partition id placeholder
CREATE TABLE t_03363_s3_sink (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet/{_partition_id}', format = Parquet, partition_strategy='hive')
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS};

-- unknown partitioning style
CREATE TABLE t_03363_s3_sink (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet', format = Parquet, partition_strategy='abc')
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS};

-- hive partition strategy can't be used without partition by clause
CREATE TABLE t_03363_s3_err (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet', partition_strategy='hive', format=Parquet); -- {serverError BAD_ARGUMENTS}

-- hive partition strategy can't be used without partition by clause
INSERT INTO FUNCTION s3(s3_conn, filename = 't_03363_parquet', partition_strategy='hive', format=Parquet) VALUES 1; -- {serverError BAD_ARGUMENTS}

-- hive partition strategy can't be used with globbed path
CREATE TABLE t_03363_s3_err (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet/**', partition_strategy='hive', format=Parquet); -- {serverError BAD_ARGUMENTS}

-- hive partition strategy can't be used with globbed path
INSERT INTO FUNCTION s3(s3_conn, filename = 't_03363_parquet/**', partition_strategy='hive', format=Parquet) VALUES 1; -- {serverError BAD_ARGUMENTS}

-- partition_columns_in_data_file can't be zero for non hive
CREATE TABLE t_03363_s3_err (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet{_partition_id}', partition_strategy='wildcard', format=Parquet, partition_columns_in_data_file=0)
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS}

-- partition_columns_in_data_file can't be zero for non hive strategy
CREATE TABLE t_03363_s3_err (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_03363_parquet', format=Parquet, partition_columns_in_data_file=0) PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS}

-- hive partition strategy can't be set in select statement?
select * from s3(s3_conn, filename='t_03363_function_write_down_partition_columns/**.parquet', format=Parquet, partition_strategy='hive'); -- {serverError BAD_ARGUMENTS}

DROP TABLE IF EXISTS t_03363_parquet, t_03363_csv;

-- do not support expressions in hive partitioning
CREATE TABLE t_invalid_expression (year UInt16, country String, counter UInt8)
                                  ENGINE = S3(s3_conn, filename = 'invalid', format = Parquet, partition_strategy='hive')
                                  PARTITION BY toString(year); -- {serverError BAD_ARGUMENTS}

-- floating types not supported
CREATE TABLE t_invalid_expression (year UInt16, country String, counter Float64)
                                  ENGINE = S3(s3_conn, filename = 'invalid', format = Parquet, partition_strategy='hive')
                                  PARTITION BY counter; -- {serverError BAD_ARGUMENTS}
