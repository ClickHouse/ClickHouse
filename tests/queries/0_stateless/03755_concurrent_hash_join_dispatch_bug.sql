-- Test for concurrent hash join with ON FALSE or missing key columns see https://github.com/ClickHouse/ClickHouse/issues/91173

DROP TABLE IF EXISTS 03755_test.t0;
DROP DATABASE IF EXISTS 03755_test;

CREATE DATABASE 03755_test;
CREATE TABLE 03755_test.t0 (c0 Int) ENGINE = Memory;
SELECT 1 FROM remote('localhost', 03755_test.t0) AS t0 JOIN 03755_test.t0 t1 ON FALSE RIGHT JOIN t0 t2 ON FALSE;

DROP TABLE 03755_test.t0;
DROP DATABASE 03755_test;

