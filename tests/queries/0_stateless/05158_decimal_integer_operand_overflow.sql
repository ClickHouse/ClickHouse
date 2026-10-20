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

-- Negative (pre-epoch) `DateTime64` and `Time64` constants must not be mistaken for overflowing operands.

SELECT toDateTime64('1969-12-31 23:59:59', 0, 'UTC') - toDateTime64('1969-12-31 23:59:58', 0, 'UTC');
SELECT materialize(toDateTime64('1969-12-31 23:59:59', 0, 'UTC')) - toDateTime64('1969-12-31 23:59:58', 0, 'UTC');
SELECT toDateTime64('1969-12-31 23:59:59', 0, 'UTC') - materialize(toDateTime64('1969-12-31 23:59:58', 0, 'UTC'));
SELECT toTime64('-00:00:01', 0) - toTime64('-00:00:02', 0) SETTINGS enable_time_time64_type = 1;
SELECT materialize(toTime64('-00:00:01', 0)) - toTime64('-00:00:02', 0) SETTINGS enable_time_time64_type = 1;
SELECT toTime('-00:00:01') - toTime64('-00:00:02', 0) SETTINGS enable_time_time64_type = 1;
SELECT toTime64('-00:00:01', 0) - toTime('-00:00:02') SETTINGS enable_time_time64_type = 1;

-- A `NULL` divisor makes the result `NULL` before an operand is narrowed.

SELECT intDiv(9223372036854775807, nullIf(toDecimal32(2, 0), toDecimal32(2, 0)));
SELECT 9223372036854775807 / nullIf(toDecimal32(2, 0), toDecimal32(2, 0));
SELECT intDiv(9223372036854775807, materialize(CAST(NULL, 'Nullable(Decimal32(0))')));
SELECT 9223372036854775807 / materialize(CAST(NULL, 'Nullable(Decimal32(0))'));
SELECT intDiv(materialize(9223372036854775807), CAST(NULL, 'Nullable(Decimal32(0))'));
SELECT intDiv(9223372036854775807, arrayJoin([NULL, toDecimal32(2, 0)])); -- { serverError DECIMAL_OVERFLOW }
