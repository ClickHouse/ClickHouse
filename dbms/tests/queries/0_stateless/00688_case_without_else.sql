DROP TABLE IF EXISTS test.test;

CREATE TABLE test.test (a UInt8) ENGINE = Memory;

INSERT INTO test.test VALUES (1), (2), (1), (3);

SELECT CASE WHEN a=1 THEN 0 END FROM test.test;

DROP TABLE test.test;
