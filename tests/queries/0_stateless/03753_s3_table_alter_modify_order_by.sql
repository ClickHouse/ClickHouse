DROP TABLE IF EXISTS test_s3;
CREATE TABLE test_s3 (c0 Int) ENGINE = S3(s3_conn, filename = 'file.csv');
ALTER TABLE test_s3 MODIFY ORDER BY (c0);
SELECT DISTINCT 1 FROM test_s3;
DROP TABLE test_s3;