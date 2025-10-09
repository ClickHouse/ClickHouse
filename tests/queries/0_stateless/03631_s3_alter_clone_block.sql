-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/87517

DROP TABLE IF EXISTS test_03631_0;
DROP TABLE IF EXISTS test_03631_1;
DROP TABLE IF EXISTS test_03631_2;
DROP TABLE IF EXISTS test_03631_hive_partition;

SET s3_create_new_file_on_insert=1;

CREATE TABLE test_03631_0 (c0 Int) ENGINE = S3(s3_conn, filename='test_03631_0_{_partition_id}', format='CSV') PARTITION BY (c0);
ALTER TABLE test_03631_0 ADD COLUMN c1 String;
-- should not result in logical error
INSERT INTO TABLE test_03631_0 (c0) VALUES (1);
INSERT INTO TABLE test_03631_0 (c1) VALUES (1);
ALTER TABLE test_03631_0 DROP COLUMN c1;
INSERT INTO TABLE test_03631_0 (c0) VALUES (1);


CREATE TABLE test_03631_1 (c0 Int, c1 Int) ENGINE = S3(s3_conn, filename='test_03631_1_{_partition_id}', format='CSV') PARTITION BY (c0);
ALTER TABLE test_03631_1 ADD COLUMN c2 Variant(Int,String);
-- should not result in logical error
INSERT INTO TABLE test_03631_1 (c0) VALUES (1);
INSERT INTO TABLE test_03631_1 (c1) VALUES (1);
INSERT INTO TABLE test_03631_1 (c2) VALUES ('test');
ALTER TABLE test_03631_1 DROP COLUMN c1;
INSERT INTO TABLE test_03631_1 (c2) VALUES ('test');
INSERT INTO TABLE test_03631_1 (c0) VALUES (1);


CREATE TABLE test_03631_2(c0 Int, c1 Nullable(Int)) ENGINE = S3(s3_conn, filename='test_03631_2_{_partition_id}', format='CSV') PARTITION BY (c0);
ALTER TABLE test_03631_2 MODIFY COLUMN c1 Date32;
-- should not result in logical error
INSERT INTO TABLE test_03631_2 (c0, c1) VALUES (0, '2020-10-10');
ALTER TABLE test_03631_2 DROP COLUMN c1;
INSERT INTO TABLE test_03631_2 (c0) VALUES (1);


CREATE TABLE test_03631_hive_partition(year Int, a Int, note String) ENGINE = S3(s3_conn, filename='03631_hive_csv', format='CSV', partition_strategy='hive') PARTITION BY (year);
INSERT INTO test_03631_hive_partition (year, a) VALUES (2020, 10);

ALTER TABLE test_03631_hive_partition ADD COLUMN extra Variant(Int, String);
INSERT INTO test_03631_hive_partition (extra, year) VALUES ('x', 2021);

ALTER TABLE test_03631_hive_partition MODIFY COLUMN extra Nullable(String);
INSERT INTO test_03631_hive_partition (extra, year) VALUES ('y', 2022);

ALTER TABLE test_03631_hive_partition DROP COLUMN a;
INSERT INTO test_03631_hive_partition (year, extra) VALUES (2023, 'z');

ALTER TABLE test_03631_hive_partition DROP COLUMN extra;
INSERT INTO test_03631_hive_partition (year) VALUES (2023);


DROP TABLE test_03631_0;
DROP TABLE test_03631_1;
DROP TABLE test_03631_2;
DROP TABLE test_03631_hive_partition;
