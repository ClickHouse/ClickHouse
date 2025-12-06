DROP TABLE IF EXISTS test_s3;
CREATE TABLE test_s3 (year UInt16, country String, counter UInt8)
    ENGINE = S3(s3_conn, filename = 't_03363_csv', format = CSV, partition_strategy='hive')
PARTITION BY (year, country);
ALTER TABLE test_s3 MODIFY ORDER BY (year);
SELECT count() FROM test_s3;
DROP TABLE test_s3;