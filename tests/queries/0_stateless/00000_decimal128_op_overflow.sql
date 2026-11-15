-- Tags: no-fasttest

DROP TABLE IF EXISTS decimal128_pow_overflow;
CREATE TABLE decimal128_pow_overflow (d128 Decimal128(0)) ENGINE=TinyLog;

-- Insert a 20-digit number, which fits in Decimal64
INSERT INTO decimal128_pow_overflow VALUES (toDecimal128(10000000000000000000, 0));

SELECT d128 * d128 from decimal128_pow_overflow;

SELECT CAST(1 AS Decimal128(0)) AS x, pow(x, 2);
SELECT CAST(10 AS Decimal128(0)) AS x, pow(x, 2);
-- SELECT CAST(10000000000000000000 AS Decimal128(4)) AS x, pow(x, 2); -- ARGUMENT_OUT_OF_BOUND

DROP TABLE decimal128_pow_overflow;

-- Insert a 39-digit number, which exceeds Decimal128
-- SELECT toDecimal128('100000000000000000000000000000000000000', 4); -- DECIMAL_OVERFLOW

-- pow() with decimals is weird, don't test it.
-- SELECT pow(d128, 2) FROM decimal128_pow_overflow;
