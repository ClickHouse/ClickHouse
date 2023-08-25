-- Tags: no-parallel

CREATE DATABASE test_00571;

USE test_00571;

DROP DATABASE IF EXISTS none;
DROP TABLE IF EXISTS test_00571;
DROP TABLE IF EXISTS test_materialized_00571;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE DATABASE none;
CREATE TABLE test_00571 ( date Date, platform Enum8('a' = 0, 'b' = 1, 'c' = 2), app Enum8('a' = 0, 'b' = 1) ) ENGINE = MergeTree(date, (platform, app), 8192);
CREATE MATERIALIZED VIEW test_materialized_00571 ENGINE = MergeTree(date, (platform, app), 8192) POPULATE AS SELECT date, platform, app FROM (SELECT * FROM test_00571);

USE none;

INSERT INTO test_00571.test_00571 VALUES('2018-02-16', 'a', 'a');

SELECT * FROM test_00571.test_00571;
SELECT * FROM test_00571.test_materialized_00571;

DETACH TABLE test_00571.test_materialized_00571;
ATTACH TABLE test_00571.test_materialized_00571;

SELECT * FROM test_00571.test_materialized_00571;

DROP DATABASE IF EXISTS none;
DROP TABLE IF EXISTS test_00571.test_00571;
DROP TABLE IF EXISTS test_00571.test_materialized_00571;

DROP DATABASE test_00571;
