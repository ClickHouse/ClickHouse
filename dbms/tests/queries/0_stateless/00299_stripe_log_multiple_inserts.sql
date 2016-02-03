DROP TABLE IF EXISTS test.log;

CREATE TABLE test.log (x UInt8) ENGINE = StripeLog;

SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (0);
SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (1);
SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (2);
SELECT * FROM test.log ORDER BY x;

DROP TABLE test.log;

CREATE TABLE test.log (x UInt8) ENGINE = TinyLog;

SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (0);
SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (1);
SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (2);
SELECT * FROM test.log ORDER BY x;

DROP TABLE test.log;

CREATE TABLE test.log (x UInt8) ENGINE = Log;

SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (0);
SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (1);
SELECT * FROM test.log ORDER BY x;
INSERT INTO test.log VALUES (2);
SELECT * FROM test.log ORDER BY x;

DROP TABLE test.log;
