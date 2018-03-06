DROP DATABASE IF EXISTS none;
DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_materialized;

USE test;

CREATE DATABASE none;
CREATE TABLE test.test ( date Date, platform Enum8('a' = 0, 'b' = 1, 'c' = 2), app Enum8('a' = 0, 'b' = 1) ) ENGINE = MergeTree(date, (platform, app), 8192);
CREATE MATERIALIZED VIEW test.test_materialized ENGINE = MergeTree(date, (platform, app), 8192) POPULATE AS SELECT date, platform, app FROM (SELECT * FROM test);

USE none;

INSERT INTO test.test VALUES('2018-02-16', 'a', 'a');

SELECT * FROM test.test;
SELECT * FROM test.test_materialized;

DETACH TABLE test.test_materialized;
ATTACH TABLE test.test_materialized;

SELECT * FROM test.test_materialized;

DROP DATABASE IF EXISTS none;
DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_materialized;
