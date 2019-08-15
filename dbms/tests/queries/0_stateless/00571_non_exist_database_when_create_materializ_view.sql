DROP DATABASE IF EXISTS none;
DROP TABLE IF EXISTS test.test_00571;
DROP TABLE IF EXISTS test.test_materialized_00571;

USE test;

CREATE DATABASE none;
CREATE TABLE test.test_00571 ( date Date, platform Enum8('a' = 0, 'b' = 1, 'c' = 2), app Enum8('a' = 0, 'b' = 1) ) ENGINE = MergeTree(date, (platform, app), 8192);
CREATE MATERIALIZED VIEW test.test_materialized_00571 ENGINE = MergeTree(date, (platform, app), 8192) POPULATE AS SELECT date, platform, app FROM (SELECT * FROM test_00571);

USE none;

INSERT INTO test.test_00571 VALUES('2018-02-16', 'a', 'a');

SELECT * FROM test.test_00571;
SELECT * FROM test.test_materialized_00571;

DETACH TABLE test.test_materialized_00571;
ATTACH TABLE test.test_materialized_00571;

SELECT * FROM test.test_materialized_00571;

DROP DATABASE IF EXISTS none;
DROP TABLE IF EXISTS test.test_00571;
DROP TABLE IF EXISTS test.test_materialized_00571;
