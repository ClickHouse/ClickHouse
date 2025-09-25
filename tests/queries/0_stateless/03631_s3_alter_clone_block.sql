-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/87517

DROP TABLE IF EXISTS test_03629_0;
DROP TABLE IF EXISTS test_03629_1;
DROP TABLE IF EXISTS test_03629_2;

SET s3_create_new_file_on_insert=1;

CREATE TABLE test_03631_0 (c0 Int) ENGINE = S3(s3_conn, filename='test_03631_0_{_partition_id}', format='CSV') PARTITION BY (c0);
ALTER TABLE test_03631_0 ADD COLUMN c1 String;
-- should not result in logical error
INSERT INTO TABLE test_03631_0 (c0) VALUES (1);


CREATE TABLE test_03631_1 (c0 Int) ENGINE = S3(s3_conn, filename='test_03631_1_{_partition_id}', format='CSV') PARTITION BY (c0);
ALTER TABLE test_03631_1 ADD COLUMN c1 Variant(Int,String);
-- should not result in logical error
INSERT INTO TABLE test_03631_1 (c0) VALUES (1);

CREATE TABLE test_03631_2(c0 Int, c1 Nullable(Int)) ENGINE = S3(s3_conn, filename='test_03631_2_{_partition_id}', format='CSV') PARTITION BY (c0);
ALTER TABLE test_03631_2 MODIFY COLUMN c1 Date32;
-- should not result in logical error
INSERT INTO TABLE test_03631_2 (c0, c1) VALUES (0, '2020-10-10');

DROP TABLE test_03631_0;
DROP TABLE test_03631_1;
DROP TABLE test_03631_2;
