-- Tags: no-parallel, no-fasttest, no-random-settings

-- Tests - Truncate for both hive partitioned and non-partitioned tables

-- Test 1: Hive partitioned table
DROP TABLE IF EXISTS test_truncate_hive;

CREATE TABLE test_truncate_hive (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 'test_truncate_hive', format = 'Parquet', partition_strategy = 'hive')
PARTITION BY (year, country);

INSERT INTO test_truncate_hive VALUES (2022, 'USA', 1), (2023, 'Canada', 2);

SELECT count() FROM test_truncate_hive;

TRUNCATE TABLE test_truncate_hive;

SELECT count() FROM test_truncate_hive;

DROP TABLE test_truncate_hive;

-- Test 2: Non-partitioned table
DROP TABLE IF EXISTS test_truncate_regular;

CREATE TABLE test_truncate_regular (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 'test_truncate_regular', format = 'Parquet');

INSERT INTO test_truncate_regular VALUES (2022, 'USA', 1), (2023, 'Canada', 2), (2024, 'Mexico', 3);

SELECT count() FROM test_truncate_regular;

TRUNCATE TABLE test_truncate_regular;

SELECT count() FROM test_truncate_regular;

DROP TABLE test_truncate_regular;
