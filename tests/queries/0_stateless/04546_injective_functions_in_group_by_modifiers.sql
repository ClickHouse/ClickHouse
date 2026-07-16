-- Tags: no-random-settings
-- Toggling optimize_injective_functions_in_group_by must not change results (#110715).
-- GROUPING SETS: the non-member set row must be kept and the rewritten key output as its default.
SELECT materialize(3) AS x
FROM numbers(10)
GROUP BY GROUPING SETS (('str'), (materialize(3)))
ORDER BY x
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT materialize(3) AS x
FROM numbers(10)
GROUP BY GROUPING SETS (('str'), (materialize(3)))
ORDER BY x
SETTINGS optimize_injective_functions_in_group_by = 1;

-- WITH TOTALS: the totals row must output the key column default, not f(default_of_argument).
SELECT toString(number) AS v, count()
FROM numbers(3)
GROUP BY v WITH TOTALS
ORDER BY v
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT toString(number) AS v, count()
FROM numbers(3)
GROUP BY v WITH TOTALS
ORDER BY v
SETTINGS optimize_injective_functions_in_group_by = 1;

-- The optimization still applies (and results are unchanged) for plain GROUP BY.
SELECT toString(number) AS v
FROM numbers(3)
GROUP BY v
ORDER BY v
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT toString(number) AS v
FROM numbers(3)
GROUP BY v
ORDER BY v
SETTINGS optimize_injective_functions_in_group_by = 1;
