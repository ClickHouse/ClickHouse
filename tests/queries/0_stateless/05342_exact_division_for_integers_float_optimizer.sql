SET optimize_arithmetic_operations_in_aggregate_functions = 1;
SET optimize_syntax_fuse_functions = 1;
SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE exact_division_float_optimizer (i Int64, f Float64, n Nullable(Float64), lc LowCardinality(Float64), d Decimal32(0)) ENGINE = Memory;
INSERT INTO exact_division_float_optimizer VALUES (1, 1, 1, 1, 1), (1, 1, NULL, 1, 1), (5, 5, 5, 5, 5), (-4, -5, -5, -5, -4);

-- Floating-point divisions keep the same aggregate rewrites with either setting value.
SET exact_division_for_integers = 0;
SELECT extract(explain, 'Aggregates: (.*)') FROM (EXPLAIN PLAN actions=1 SELECT sum(f / 3), avg(f / 3), min(f / -3), max(f / -3), sum(i / 3.0), sum(n / 3), sum(lc / 3) FROM exact_division_float_optimizer) WHERE position(explain, 'Aggregates:') > 0;
SELECT sum(f / 3), avg(f / 3), min(f / -3), max(f / -3), sum(i / 3.0), sum(n / 3), sum(lc / 3) FROM exact_division_float_optimizer;

SET exact_division_for_integers = 1;
SELECT extract(explain, 'Aggregates: (.*)') FROM (EXPLAIN PLAN actions=1 SELECT sum(f / 3), avg(f / 3), min(f / -3), max(f / -3), sum(i / 3.0), sum(n / 3), sum(lc / 3) FROM exact_division_float_optimizer) WHERE position(explain, 'Aggregates:') > 0;
SELECT sum(f / 3), avg(f / 3), min(f / -3), max(f / -3), sum(i / 3.0), sum(n / 3), sum(lc / 3) FROM exact_division_float_optimizer;

-- Integer and decimal divisions must retain per-row truncation.
SELECT extract(explain, 'Aggregates: (.*)') FROM (EXPLAIN PLAN actions=1 SELECT sum(i / 3), avg(i / 3), sum(d / 3), avg(d / 3) FROM exact_division_float_optimizer) WHERE position(explain, 'Aggregates:') > 0;
SELECT sum(i / 3), avg(i / 3), sum(d / 3), avg(d / 3) FROM exact_division_float_optimizer;
DROP TABLE exact_division_float_optimizer;
