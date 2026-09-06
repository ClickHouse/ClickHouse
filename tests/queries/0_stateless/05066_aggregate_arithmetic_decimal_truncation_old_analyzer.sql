-- The same for the old analyzer, which cannot see the argument types and therefore declines the rewrite
-- for every literal that does not fit the narrowest native width of a `Decimal`.

SET enable_analyzer = 0;

DROP TABLE IF EXISTS t_aggregate_arithmetic_decimal_old;
CREATE TABLE t_aggregate_arithmetic_decimal_old (a Decimal(5, 4)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_aggregate_arithmetic_decimal_old SELECT toDecimal32(number, 4) FROM numbers(10);

SELECT min(a * 9223372036854775807), max(a * 9223372036854775807), sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal_old;
SELECT min(a * 9223372036854775807), max(a * 9223372036854775807), sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal_old
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

EXPLAIN SYNTAX SELECT sum(a * 4294967296) FROM t_aggregate_arithmetic_decimal_old;
EXPLAIN SYNTAX SELECT sum(a * 2) FROM t_aggregate_arithmetic_decimal_old;

DROP TABLE t_aggregate_arithmetic_decimal_old;
