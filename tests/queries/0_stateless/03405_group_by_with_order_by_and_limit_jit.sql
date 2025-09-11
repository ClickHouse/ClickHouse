-- Aggregate functions are JIT-compiled after min_count_to_compile_aggregate_expression queries, if compile_aggregate_expressions is set to true.
-- This test verifies that after JIT compilation, queries are executed correctly considering the implemented optimization https://github.com/ClickHouse/ClickHouse/issues/72610.
SET compile_aggregate_expressions = 1;
SET min_count_to_compile_expression = 3;
DROP TABLE IF EXISTS t;
CREATE TABLE t(a Int64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3), (4), (5), (6);
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
SELECT a, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
