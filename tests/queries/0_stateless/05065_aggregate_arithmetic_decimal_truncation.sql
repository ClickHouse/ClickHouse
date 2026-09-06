-- Arithmetic with a `Decimal` operand computes in the decimal's own native signed width, into which the
-- other operand is cast: `Decimal32 * 9223372036854775807` multiplies by `-1`. Hoisting the constant out
-- of the aggregate must not fire then - the `min`/`max` swap decision would use the wrong sign, and the
-- hoisted operation would compute in the wider result type of the aggregate.

DROP TABLE IF EXISTS t_aggregate_arithmetic_decimal;
CREATE TABLE t_aggregate_arithmetic_decimal (a Decimal(5, 4)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_aggregate_arithmetic_decimal SELECT toDecimal32(number, 4) FROM numbers(10);

SELECT min(a * 9223372036854775807), max(a * 9223372036854775807), sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal;
SELECT min(a * 9223372036854775807), max(a * 9223372036854775807), sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- A constant that fits the native width is hoisted as before, and so is one used with an integer column.
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a * 2) FROM t_aggregate_arithmetic_decimal);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal);
SELECT extract(arrayStringConcat(groupArray(explain), ' '), 'function_name: (multiply|sum)') AS outer_function
FROM (EXPLAIN QUERY TREE SELECT sum(number * 4294967296) FROM numbers(10));

DROP TABLE t_aggregate_arithmetic_decimal;
