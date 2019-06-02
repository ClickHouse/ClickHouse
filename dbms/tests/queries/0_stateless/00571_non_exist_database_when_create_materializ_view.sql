CREATE DATABASE none;
CREATE TABLE test ( date Date, platform Enum8('a' = 0, 'b' = 1, 'c' = 2), app Enum8('a' = 0, 'b' = 1) ) ENGINE = MergeTree(date, (platform, app), 8192);
CREATE MATERIALIZED VIEW test_materialized ENGINE = MergeTree(date, (platform, app), 8192) POPULATE AS SELECT date, platform, app FROM (SELECT * FROM test);

USE none;

INSERT INTO default.test VALUES('2018-02-16', 'a', 'a');

SELECT * FROM default.test;
SELECT * FROM default.test_materialized;

DETACH TABLE default.test_materialized;
ATTACH TABLE default.test_materialized;

SELECT * FROM default.test_materialized;
