CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal_sum;
CREATE TABLE test.decimal_sum
(
    date Date,
    sum32 Decimal32(4),
    sum64 Decimal64(8),
    sum128 Decimal128(10)
) Engine = SummingMergeTree(date, (date), 8192);

INSERT INTO test.decimal_sum VALUES ('2001-01-01', 1, 1, -1);
INSERT INTO test.decimal_sum VALUES ('2001-01-01', 1, -1, -1);

OPTIMIZE TABLE test.decimal_sum;
SELECT * FROM test.decimal_sum;

INSERT INTO test.decimal_sum VALUES ('2001-01-01', -2, 1, 2);

OPTIMIZE TABLE test.decimal_sum;
SELECT * FROM test.decimal_sum;

INSERT INTO test.decimal_sum VALUES ('2001-01-01', 0, -1, 0);

OPTIMIZE TABLE test.decimal_sum;
SELECT * FROM test.decimal_sum;

drop table test.decimal_sum;
