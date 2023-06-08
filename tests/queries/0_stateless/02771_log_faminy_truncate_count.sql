-- Tags: no-parallel
DROP TABLE IF EXISTS default.test_log;
CREATE TABLE default.test_log
(
    `crypto_name` String,
    `trade_date` Date
)
ENGINE = Log;

INSERT INTO default.test_log (crypto_name, trade_date) VALUES ('abc', '2021-01-01'), ('def', '2022-02-02');

TRUNCATE TABLE default.test_log;
SELECT count() FROM  default.test_log;

DROP TABLE IF EXISTS default.test_log;
CREATE TABLE default.test_log
(
    `crypto_name` String,
    `trade_date` Date
)
ENGINE = StripeLog;

INSERT INTO default.test_log (crypto_name, trade_date) VALUES ('abc', '2021-01-01'), ('def', '2022-02-02');

TRUNCATE TABLE default.test_log;
SELECT count() FROM  default.test_log;
DROP TABLE default.test_log;
