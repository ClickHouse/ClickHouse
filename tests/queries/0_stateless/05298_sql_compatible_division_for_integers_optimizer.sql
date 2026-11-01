SET sql_compatible_division_for_integers = 1;
CREATE TABLE exact_division_optimizer (x Int64) ENGINE = MergeTree ORDER BY x;
INSERT INTO exact_division_optimizer VALUES (1), (1), (5), (-5), (0);

SELECT sum(x / 2), avg(x / 2), min(x / 2), max(x / 2) FROM exact_division_optimizer SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT sum(x / 2), avg(x / 2), min(x / 2), max(x / 2) FROM exact_division_optimizer SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT sum(x / 2), avg(x / 2), min(x / 2), max(x / 2) FROM exact_division_optimizer SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT x / 3 AS q, count() FROM exact_division_optimizer GROUP BY q ORDER BY q;
SELECT count() FROM exact_division_optimizer WHERE x / 3 = 0;
SELECT DISTINCT x / 3 AS q FROM exact_division_optimizer ORDER BY q;
SELECT sum(x / 2), toTypeName(sum(x / 2)) FROM exact_division_optimizer SETTINGS sql_compatible_division_for_integers = 0;
DROP TABLE exact_division_optimizer;

-- A stored floating-point expression must not satisfy an integer-division predicate.
SET sql_compatible_division_for_integers = 0;
CREATE TABLE exact_division_key (x Int64) ENGINE = MergeTree PARTITION BY x ORDER BY x / 2;
INSERT INTO exact_division_key VALUES (-1), (0), (1), (2);
SELECT groupArray(x) FROM (SELECT x FROM exact_division_key WHERE x / 2 = 0 ORDER BY x) SETTINGS sql_compatible_division_for_integers = 1;
SELECT groupArray(x) FROM (SELECT x FROM exact_division_key WHERE x / 2 = 0 ORDER BY x) SETTINGS sql_compatible_division_for_integers = 1;
SELECT groupArray(x) FROM (SELECT x FROM exact_division_key WHERE x / 2 = 0 ORDER BY x) SETTINGS sql_compatible_division_for_integers = 0;
DROP TABLE exact_division_key;
