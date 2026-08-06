-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100740
-- JIT-compiled MIN/MAX on Decimal columns used unsigned comparison, producing wrong results
-- after the JIT threshold (min_count_to_compile_aggregate_expression) was reached.

SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

DROP TABLE IF EXISTS t_decimal_minmax;
CREATE TABLE t_decimal_minmax (grp String, val Decimal(38, 26)) ENGINE = MergeTree() ORDER BY grp;
INSERT INTO t_decimal_minmax VALUES ('a', 5), ('a', -10), ('a', 3), ('a', -1), ('a', 7);

SELECT max(val) FROM t_decimal_minmax GROUP BY grp;
SELECT min(val) FROM t_decimal_minmax GROUP BY grp;

-- Run multiple times to ensure JIT-compiled path is also correct.
SELECT max(val) FROM t_decimal_minmax GROUP BY grp;
SELECT min(val) FROM t_decimal_minmax GROUP BY grp;
SELECT max(val) FROM t_decimal_minmax GROUP BY grp;
SELECT min(val) FROM t_decimal_minmax GROUP BY grp;

DROP TABLE t_decimal_minmax;
