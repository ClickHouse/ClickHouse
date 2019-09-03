DROP TABLE IF EXISTS test.test_log;
DROP TABLE IF EXISTS test.test_tiny_log;
DROP TABLE IF EXISTS test.test_stripe_log;

CREATE TABLE test.test_log(x UInt64) ENGINE = Log;
CREATE TABLE test.test_tiny_log(x UInt64) ENGINE = TinyLog;
CREATE TABLE test.test_stripe_log(x UInt64) ENGINE = StripeLog;

INSERT INTO test.test_log VALUES(1)(2);
INSERT INTO test.test_tiny_log VALUES(1)(2);
INSERT INTO test.test_stripe_log VALUES(1)(2);

SELECT * FROM test.test_log;
SELECT * FROM test.test_tiny_log;
SELECT * FROM test.test_stripe_log;

INSERT INTO test.test_log SELECT * FROM test.test_log;
INSERT INTO test.test_tiny_log SELECT * FROM test.test_tiny_log;
INSERT INTO test.test_stripe_log SELECT * FROM test.test_stripe_log;

SELECT * FROM test.test_log;
SELECT * FROM test.test_tiny_log;
SELECT * FROM test.test_stripe_log;

DROP TABLE IF EXISTS test.test_log;
DROP TABLE IF EXISTS test.test_tiny_log;
DROP TABLE IF EXISTS test.test_stripe_log;
