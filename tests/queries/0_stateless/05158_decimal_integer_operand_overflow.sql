-- The arithmetic runs in the native width of the decimal result, so an integer operand that does not
-- fit that width used to wrap and make every row silently wrong. Comparing the same pair already
-- reports `DECIMAL_OVERFLOW`.

SELECT intDiv(toDecimal32(1.5, 4), -9223372036854775807); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(1.5, 4) / -9223372036854775807; -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(1.5, 4) + 9223372036854775807; -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(1.5, 4) - 9223372036854775807; -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(1.5, 4) * 9223372036854775807; -- { serverError DECIMAL_OVERFLOW }
SELECT intDiv(9223372036854775807, toDecimal32(2, 4)); -- { serverError DECIMAL_OVERFLOW }

DROP TABLE IF EXISTS t_decimal_operand;
CREATE TABLE t_decimal_operand (a Decimal(5, 4), b Int64) ENGINE = Memory;
INSERT INTO t_decimal_operand VALUES (1.5, 9223372036854775807);

SELECT a * b FROM t_decimal_operand; -- { serverError DECIMAL_OVERFLOW }
SELECT a * materialize(9223372036854775807) FROM t_decimal_operand; -- { serverError DECIMAL_OVERFLOW }
SELECT a + b FROM t_decimal_operand; -- { serverError DECIMAL_OVERFLOW }

-- An operand that fits is unaffected, and so is a wider decimal.

SELECT a * 2, a + 3, a - 1, a / 2, intDiv(a, 1) FROM t_decimal_operand;
SELECT toDecimal128(1.5, 4) * 9223372036854775807;
SELECT toDecimal64(1.5, 4) * 1000000;
SELECT toDecimal32(1.5, 4) * 100000;

DROP TABLE t_decimal_operand;
