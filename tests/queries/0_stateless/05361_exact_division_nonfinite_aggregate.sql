SET sql_compatible_division_for_integers = 1;
SET optimize_syntax_fuse_functions = 0;

-- Moving division out of the aggregate changes infinities, signed zero, and empty sums.
SELECT min(x / -0.0), max(x / -0.0), isNaN(sum(x / 0.0)), isNaN(avg(x / 0.0)) FROM values('x Float64', (-1), (2)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT min(x / -0.0), max(x / -0.0), isNaN(sum(x / 0.0)), isNaN(avg(x / 0.0)) FROM values('x Float64', (-1), (2)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT sum(x / inf), avg(x / inf) FROM values('x Float64', (1e308), (1e308)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT sum(x / inf), avg(x / inf) FROM values('x Float64', (1e308), (1e308)) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT sum(x / nan), sum(x / 0.0) FROM values('x Float64', (1)) WHERE 0 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT sum(x / nan), sum(x / 0.0) FROM values('x Float64', (1)) WHERE 0 SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
