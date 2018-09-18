SET allow_experimental_decimal_type = 1;
SET send_logs_level = 'none';

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal;

CREATE TABLE IF NOT EXISTS test.decimal
(
    a DECIMAL(9,4) DEFAULT 0,
    b DECIMAL(18,4) DEFAULT a / 2,
    c DECIMAL(38,4) DEFAULT b / 3,
    d MATERIALIZED a + toDecimal32('0.2', 1),
    e ALIAS b * 2,
    f ALIAS c * 6
) ENGINE = Memory;

DESC TABLE test.decimal;

INSERT INTO test.decimal (a) VALUES (0), (1), (2), (3);
SELECT * FROM test.decimal;
SELECT a, b, c, d, e, f FROM test.decimal;

DROP TABLE IF EXISTS test.decimal;
