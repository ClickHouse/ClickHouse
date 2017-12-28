DROP TABLE IF EXISTS test.test;

CREATE TABLE test.test(number UInt64, num2 UInt64) ENGINE = Log;

INSERT INTO test.test WITH number * 2 AS num2 SELECT number, num2 FROM system.numbers LIMIT 3;

SELECT * FROM test.test;

DROP TABLE test.test;
