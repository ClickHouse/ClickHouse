-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

DROP TABLE IF EXISTS test_s3;
set s3_truncate_on_insert = 1;
CREATE TABLE test_s3 (c0 Int) ENGINE = S3(s3_conn, filename = 'file.csv');
INSERT INTO test_s3 VALUES(1);
ALTER TABLE test_s3 MODIFY ORDER BY (c0);
SELECT * FROM test_s3;
DROP TABLE test_s3;