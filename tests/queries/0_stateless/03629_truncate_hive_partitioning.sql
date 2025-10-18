-- Tests truncate hive partitioning

DROP TABLE IF EXISTS test_truncate_hive;

CREATE TABLE test_truncate_hive (year UInt16, country String, counter UInt8)
ENGINE = S3(s3_conn, filename = 'test_truncate_hive', format = 'Parquet', partition_strategy = 'hive')
PARTITION BY (year, country);

INSERT INTO test_truncate_hive VALUES (2022, 'USA', 1), (2023, 'Canada', 2);

SELECT count() FROM test_truncate_hive;

TRUNCATE TABLE test_truncate_hive;

SELECT count() FROM test_truncate_hive;

DROP TABLE test_truncate_hive;
