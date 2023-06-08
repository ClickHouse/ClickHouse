DROP TABLE IF EXISTS test_log;
CREATE TABLE test_log
(
    `crypto_name` String,
    `trade_date` Date
)
ENGINE = Log;

INSERT INTO test_log (crypto_name, trade_date) VALUES ('abc', '2021-01-01'), ('def', '2022-02-02');

TRUNCATE TABLE test_log;
SELECT count() FROM  test_log;

DROP TABLE IF EXISTS test_log;
CREATE TABLE test_log
(
    `crypto_name` String,
    `trade_date` Date
)
ENGINE = StripeLog;

INSERT INTO test_log (crypto_name, trade_date) VALUES ('abc', '2021-01-01'), ('def', '2022-02-02');

TRUNCATE TABLE test_log;
SELECT count() FROM  test_log;
DROP TABLE test_log;
