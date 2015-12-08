DROP TABLE IF EXISTS test.stripelog;
CREATE TABLE test.stripelog (x UInt8) ENGINE = StripeLog;

SELECT * FROM test.stripelog ORDER BY x;
INSERT INTO test.stripelog VALUES (1), (2);
SELECT * FROM test.stripelog ORDER BY x;

DROP TABLE test.stripelog;
