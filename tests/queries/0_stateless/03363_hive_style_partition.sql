-- Tags: no-parallel, no-fasttest, no-random-settings

SET allow_suspicious_low_cardinality_types=1;

DROP TABLE IF EXISTS t_parquet_03363, t_csv_03363, t_mixed_partitioning_03363, s3_table_half_schema_with_format, s3_table_infer_schema, s3_table_infer_schema_hive_strategy;

CREATE TABLE t_parquet_03363 (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_parquet_03363', format = Parquet, partition_strategy='hive')
PARTITION BY (year, country);

INSERT INTO t_parquet_03363 VALUES
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
select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.parquet', '/<snowflakeid>.parquet') AS _path, counter from t_parquet_03363 order by counter;

-- CSV test
CREATE TABLE t_csv_03363 (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_csv_03363', format = CSV, partition_strategy='hive')
PARTITION BY (year, country);

INSERT INTO t_csv_03363 VALUES
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

select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.csv', '/<snowflakeid>.csv') AS _path, counter from t_csv_03363 order by counter;

-- s3 table function
INSERT INTO FUNCTION s3(s3_conn, filename='t_function_03363', format=Parquet, partition_strategy='hive') PARTITION BY (year, country) SELECT country, year, counter FROM t_parquet_03363;
select distinct on (counter) replaceRegexpAll(_path, '/[0-9]+\\.parquet', '/<snowflakeid>.parquet') AS _path, counter from s3(s3_conn, filename='t_function_03363/**.parquet') order by counter;

-- Should succeed because it is not using the hive partition strategy
CREATE TABLE s3_table_infer_schema ENGINE=S3(s3_conn, filename='t_function_03363/**.parquet', format=Parquet);

SHOW CREATE TABLE s3_table_infer_schema;

SELECT COUNT(distinct counter) FROM s3_table_infer_schema;

-- Should fail because hive partition strategy requires the schema to be manually specified
CREATE TABLE s3_table_infer_schema_hive_strategy ENGINE=S3(s3_conn, filename='t_function_03363', format=Parquet, partition_strategy='hive') PARTITION BY (year, country); -- {serverError UNKNOWN_IDENTIFIER};

-- create a "bucket" with mixed partitioning schemes so we can simulate a malformed storage
INSERT INTO FUNCTION s3(s3_conn, filename='t_mixed_partitioning_03363', format=Parquet, partition_strategy='hive') PARTITION BY (year) select 1 as id, 2025 as year;
INSERT INTO FUNCTION s3(s3_conn, filename='t_mixed_partitioning_03363', format=Parquet, partition_strategy='hive') PARTITION BY (country) select 1 as id, 'Brazil' as country;

-- Depends on the above two inserts, should throw exception because it could not find the hive partition columns it was looking for
-- The format is null because one of the files contains the requested columns and might return the data before we throw the exception
select * from s3(s3_conn, filename='t_mixed_partitioning_03363/**.parquet') Format null; -- {serverError INCORRECT_DATA}

-- Depends on the above two inserts, should throw exception because it could not find the hive partition columns it was looking for
-- The format is null because one of the files contains the requested columns and might return the data before we throw the exception
CREATE TABLE t_mixed_partitioning_03363 (id Int32, year UInt16) ENGINE=S3(s3_conn, filename='t_mixed_partitioning_03363', format=Parquet, partition_strategy='hive') PARTITION BY (year);
SELECT * FROM t_mixed_partitioning_03363 Format null; -- {serverError INCORRECT_DATA}

-- should output 1 because partition columns are not written down to the file by default when hive style is being used
select num_columns from s3(s3_conn, filename='t_function_03363/**.parquet', format=ParquetMetadata) limit 1;

INSERT INTO FUNCTION s3(s3_conn, filename='t_function_write_down_partition_columns_03363', format=Parquet, partition_strategy='hive', partition_columns_in_data_file=1) PARTITION BY (year, country) SELECT country, year, counter FROM t_parquet_03363;
select num_columns from s3(s3_conn, filename='t_function_write_down_partition_columns_03363/**.parquet', format=ParquetMetadata) limit 1;

-- hive partitioning = 0 so we know it is not reading columns from the path
select * from s3(s3_conn, filename='t_function_write_down_partition_columns_03363/**.parquet', format=Parquet) order by counter limit 1 SETTINGS use_hive_partitioning=0;

-- only partition columns
INSERT INTO FUNCTION s3(s3_conn, filename='t_parquet_03363', format=Parquet, partition_strategy='hive') PARTITION BY (year, country) SELECT 2020 as year, 'Brazil' as country; -- {serverError INCORRECT_DATA};

-- Schema specified, but the hive partition column is missing in the schema (present in the data tho)
INSERT INTO FUNCTION s3(s3_conn, filename='half_baked', format=Parquet, partition_strategy='hive') PARTITION BY year SELECT 1 AS key, 2020 AS year;

-- Partition columns shall be enriched
CREATE TABLE s3_table_half_schema_with_format (key UInt64) engine=S3(s3_conn, filename='half_baked/**.parquet', format=Parquet);

-- Should succeed and return hive columns. Distinct because maybe MinIO isn't cleaned up
SELECT DISTINCT * FROM s3_table_half_schema_with_format;

-- Incompatible partition columns vs manually specified schema
CREATE TABLE s3_table_incompatible_partition_by (key UInt64) engine=S3(s3_conn, filename='t_03363_function/**.parquet', format=Parquet) PARTITION BY (abracadabra); -- {serverError UNKNOWN_IDENTIFIER}

-- Incompatible partition columns vs manually specified schema
-- CREATE TABLE s3_table_incompatible_partition_by (key UInt64) engine=S3(s3_conn, filename='t_03363_function', format=Parquet, partition_strategy='hive') PARTITION BY (abracadabra); -- {serverError UNKNOWN_IDENTIFIER}

-- hive with partition id placeholder
CREATE TABLE t_s3_sink_03363 (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_parquet_03363/{_partition_id}', format = Parquet, partition_strategy='hive')
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS};

-- unknown partitioning style
CREATE TABLE t_s3_sink_03363 (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_parquet_03363', format = Parquet, partition_strategy='abc')
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS};

-- hive partition strategy can't be used without partition by clause
CREATE TABLE t_s3_err_03363 (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_parquet_03363', partition_strategy='hive', format=Parquet); -- {serverError BAD_ARGUMENTS}

-- hive partition strategy can't be used without partition by clause
INSERT INTO FUNCTION s3(s3_conn, filename = 't_parquet_03363', partition_strategy='hive', format=Parquet) VALUES 1; -- {serverError BAD_ARGUMENTS}

-- hive partition strategy can't be used with globbed path
CREATE TABLE t_s3_err_03363 (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_parquet_03363/**', partition_strategy='hive', format=Parquet); -- {serverError BAD_ARGUMENTS}

-- hive partition strategy can't be used with globbed path
INSERT INTO FUNCTION s3(s3_conn, filename = 't_parquet_03363/**', partition_strategy='hive', format=Parquet) VALUES 1; -- {serverError BAD_ARGUMENTS}

-- partition_columns_in_data_file can't be zero for non hive
CREATE TABLE t_s3_err_03363 (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_parquet_03363{_partition_id}', partition_strategy='wildcard', format=Parquet, partition_columns_in_data_file=0)
PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS}

-- partition_columns_in_data_file can't be zero for non hive strategy
CREATE TABLE t_s3_err_03363 (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 't_parquet_03363', format=Parquet, partition_columns_in_data_file=0) PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS}

-- hive partition strategy can't be set in select statement?
select * from s3(s3_conn, filename='err/**.parquet', format=Parquet, partition_strategy='hive'); -- {serverError BAD_ARGUMENTS}

-- do not support expressions in hive partitioning
CREATE TABLE t_invalid_expression (year UInt16, country String, counter UInt8)
                                  ENGINE = S3(s3_conn, filename = 'invalid', format = Parquet, partition_strategy='hive')
                                  PARTITION BY toString(year); -- {serverError BAD_ARGUMENTS}

-- floating types not supported
CREATE TABLE t_invalid_expression (year UInt16, country String, counter Float64)
                                  ENGINE = S3(s3_conn, filename = 'invalid', format = Parquet, partition_strategy='hive')
                                  PARTITION BY counter; -- {serverError BAD_ARGUMENTS}

-- Data lake like engines do not support the `partition_strategy` argument
CREATE TABLE t_iceberg_03363 ENGINE=IcebergS3(s3_conn, filename = 'iceberg_data/default/t_iceberg/', format='parquet', url = 'http://minio1:9001/bucket/', partition_strategy='WILDCARD'); -- {serverError BAD_ARGUMENTS}
CREATE TABLE t_iceberg_03363 ENGINE=IcebergS3(s3_conn, filename = 'iceberg_data/default/t_iceberg/', format='parquet', url = 'http://minio1:9001/bucket/', partition_strategy='HIVE'); -- {serverError BAD_ARGUMENTS}

-- Should throw because format is not present and it is mandatory for hive strategy and it should not be a LOGICAL_ERROR
CREATE TABLE t_hive_requires_format_03363 (c0 Int) ENGINE = S3(s3_conn, partition_strategy = 'hive') PARTITION BY (c0); -- {serverError BAD_ARGUMENTS}

-- Should throw because hive strategy does not allow partition columns to be the only columns
CREATE TABLE t_hive_only_partition_columns_03363 (country String, year UInt16) ENGINE = S3(s3_conn, partition_strategy='hive') PARTITION BY (year, country); -- {serverError BAD_ARGUMENTS};

DROP TABLE IF EXISTS t_parquet_03363, t_csv_03363, s3_table_half_schema_with_format, s3_table_infer_schema, s3_table_infer_schema_hive_strategy;
