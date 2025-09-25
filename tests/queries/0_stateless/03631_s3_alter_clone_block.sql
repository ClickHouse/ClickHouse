-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/87517

DROP TABLE IF EXISTS test_03629;

SET s3_create_new_file_on_insert=1;

CREATE TABLE test_03631 (c0 Int) ENGINE = S3(s3_conn, filename='test_03631_{_partition_id}', format='CSV') PARTITION BY (c0);
ALTER TABLE test_03631 ADD COLUMN c1 String;
INSERT INTO TABLE test_03631 (c0) VALUES (1);
SELECT COUNT() FROM test_03631;

DROP TABLE test_03631;
