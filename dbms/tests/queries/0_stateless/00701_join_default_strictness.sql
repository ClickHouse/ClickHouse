CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.a1;
DROP TABLE IF EXISTS test.a2;

SET send_logs_level = 'none';

CREATE TABLE test.a1(a UInt8, b UInt8) ENGINE=Memory;
CREATE TABLE test.a2(a UInt8, b UInt8) ENGINE=Memory;

INSERT INTO test.a1 VALUES (1, 1);
INSERT INTO test.a1 VALUES (1, 2);
INSERT INTO test.a1 VALUES (1, 3);
INSERT INTO test.a2 VALUES (1, 2);
INSERT INTO test.a2 VALUES (1, 3);
INSERT INTO test.a2 VALUES (1, 4);

SELECT a, b FROM test.a1 LEFT JOIN (SELECT a, b FROM test.a2) USING a ORDER BY b; -- { serverError 417 }

SELECT a, b FROM test.a1 LEFT JOIN (SELECT a, b FROM test.a2) USING a ORDER BY b SETTINGS join_default_strictness='ANY';

SELECT a, b FROM test.a1 LEFT JOIN (SELECT a, b FROM test.a2) USING a ORDER BY b SETTINGS join_default_strictness='ALL';

DROP TABLE IF EXISTS test.a1;
DROP TABLE IF EXISTS test.a2;
