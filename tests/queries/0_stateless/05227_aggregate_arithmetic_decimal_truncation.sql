-- Arithmetic with a `Decimal` operand does not distribute over `sum`, `min`, `max` and `avg`, so the
-- constant must not be hoisted out of the aggregate then. The operation computes in the decimal's own
-- native signed width, into which the other operand is cast (`Decimal32 * 9223372036854775807` multiplies
-- by `-1`, and an `Int64` column times a `Decimal32` constant wraps around above `2^31 - 1`), it checks
-- every row for overflow, and `divide` drops the fractional digits beyond the scale on every row. The
-- hoisted operation would run once, on the aggregate, in its wider result type.

SET optimize_arithmetic_operations_in_aggregate_functions = 1;

DROP TABLE IF EXISTS t_aggregate_arithmetic_decimal;
CREATE TABLE t_aggregate_arithmetic_decimal (a Decimal(5, 4)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_aggregate_arithmetic_decimal SELECT toDecimal32(number, 4) FROM numbers(10);

SELECT min(a * 9223372036854775807), max(a * 9223372036854775807), sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal;
SELECT min(a * 9223372036854775807), max(a * 9223372036854775807), sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- The sibling rewrite of `sum(column +/- literal)` into `sum(column) +/- literal * count(column)` is
-- guarded by the same invariant: `a + 4294967296` adds `0` to every `Decimal32` row.
SELECT sum(a + 4294967296), sum(a - 4294967296), sum(4294967296 - a), sum(4294967296 + a) FROM t_aggregate_arithmetic_decimal;
SELECT sum(a + 4294967296), sum(a - 4294967296), sum(4294967296 - a), sum(4294967296 + a) FROM t_aggregate_arithmetic_decimal
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- The mirrored shape: an integer column wider than the native width of the `Decimal` constant.
DROP TABLE IF EXISTS t_aggregate_arithmetic_int;
CREATE TABLE t_aggregate_arithmetic_int (a Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_aggregate_arithmetic_int VALUES (4294967298), (5);

SELECT min(a * toDecimal32(1, 0)), max(a * toDecimal32(1, 0)), avg(a * toDecimal32(1, 0)) FROM t_aggregate_arithmetic_int;
SELECT min(a * toDecimal32(1, 0)), max(a * toDecimal32(1, 0)), avg(a * toDecimal32(1, 0)) FROM t_aggregate_arithmetic_int
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- A constant that fits the native width still overflows it on a row: the original query throws, the
-- hoisted one would not.
DROP TABLE IF EXISTS t_aggregate_arithmetic_overflow;
CREATE TABLE t_aggregate_arithmetic_overflow (a Decimal32(0)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_aggregate_arithmetic_overflow VALUES (999999999);

SELECT sum(a + 1200000000) FROM t_aggregate_arithmetic_overflow; -- { serverError DECIMAL_OVERFLOW }
SELECT sum(a * 3) FROM t_aggregate_arithmetic_overflow; -- { serverError DECIMAL_OVERFLOW }
SELECT max(a + 1200000000) FROM t_aggregate_arithmetic_overflow; -- { serverError DECIMAL_OVERFLOW }
SELECT avg(a * 3) FROM t_aggregate_arithmetic_overflow; -- { serverError DECIMAL_OVERFLOW }

-- `Decimal` division drops the fractional digits beyond the scale on every row.
DROP TABLE IF EXISTS t_aggregate_arithmetic_divide;
CREATE TABLE t_aggregate_arithmetic_divide (k UInt8, a Decimal32(0)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_aggregate_arithmetic_divide VALUES (0, 1), (0, 1), (1, 1), (1, 2);

SELECT k, sum(a / 2), avg(a / 2) FROM t_aggregate_arithmetic_divide GROUP BY k ORDER BY k;
SELECT k, sum(a / 2), avg(a / 2) FROM t_aggregate_arithmetic_divide GROUP BY k ORDER BY k
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- No rewrite fires when an operand is a `Decimal`, whatever the constant.
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|min)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT min(a * toDecimal32(1, 0)) FROM t_aggregate_arithmetic_int);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a * 2) FROM t_aggregate_arithmetic_decimal);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (divide|avg)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT avg(a / 2) FROM t_aggregate_arithmetic_divide);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (plus|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a + 4294967296) FROM t_aggregate_arithmetic_decimal);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (minus|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(4294967296 - a) FROM t_aggregate_arithmetic_decimal);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (plus|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a + 2) FROM t_aggregate_arithmetic_decimal);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (plus|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a + toDecimal64(1, 0)) FROM t_aggregate_arithmetic_int);

-- Without a `Decimal` operand, the constant is hoisted as before.
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(number * 4294967296) FROM numbers(10));
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|min)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT min(a * 2) FROM t_aggregate_arithmetic_int);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (plus|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a + 2) FROM t_aggregate_arithmetic_int);

DROP TABLE t_aggregate_arithmetic_decimal;
DROP TABLE t_aggregate_arithmetic_int;
DROP TABLE t_aggregate_arithmetic_overflow;
DROP TABLE t_aggregate_arithmetic_divide;
