-- Arithmetic with a `Decimal` operand computes in the decimal's own native signed width, into which the
-- other operand is cast: `Decimal32 * 9223372036854775807` multiplies by `-1`, and an `Int64` column
-- times a `Decimal32` constant wraps around above `2^31 - 1`. Hoisting the constant out of the aggregate
-- must not fire then - the `min`/`max` swap decision would use the wrong sign, and the hoisted operation
-- would compute in the wider result type of the aggregate.

SET optimize_arithmetic_operations_in_aggregate_functions = 1;

DROP TABLE IF EXISTS t_aggregate_arithmetic_decimal;
CREATE TABLE t_aggregate_arithmetic_decimal (a Decimal(5, 4)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_aggregate_arithmetic_decimal SELECT toDecimal32(number, 4) FROM numbers(10);

SELECT min(a * 9223372036854775807), max(a * 9223372036854775807), sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal;
SELECT min(a * 9223372036854775807), max(a * 9223372036854775807), sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- The mirrored shape: an integer column wider than the native width of the `Decimal` constant.
DROP TABLE IF EXISTS t_aggregate_arithmetic_int;
CREATE TABLE t_aggregate_arithmetic_int (a Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_aggregate_arithmetic_int VALUES (4294967298), (5);

SELECT min(a * toDecimal32(1, 0)), max(a * toDecimal32(1, 0)), avg(a * toDecimal32(1, 0)) FROM t_aggregate_arithmetic_int;
SELECT min(a * toDecimal32(1, 0)), max(a * toDecimal32(1, 0)), avg(a * toDecimal32(1, 0)) FROM t_aggregate_arithmetic_int
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|min)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT min(a * toDecimal32(1, 0)) FROM t_aggregate_arithmetic_int);

-- A constant that fits the native width is hoisted as before, and so is one used with an integer column
-- narrow enough for the width of the `Decimal` constant.
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a * 2) FROM t_aggregate_arithmetic_decimal);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(number * 4294967296) FROM numbers(10));
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|min)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT min(toInt16(a) * toDecimal32(1, 0)) FROM t_aggregate_arithmetic_int);

DROP TABLE t_aggregate_arithmetic_decimal;
DROP TABLE t_aggregate_arithmetic_int;
