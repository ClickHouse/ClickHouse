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

-- Same invariant for the sibling optimize_group_by_function_keys pass, which drops a key that is a
-- function of other keys. With WITH TOTALS the dropped key must be output as its column default in
-- the totals row, not recomputed from the other keys' totals-row defaults (#110715).
SELECT toString(number) AS v, number, count()
FROM numbers(3)
GROUP BY v, number WITH TOTALS
ORDER BY number
SETTINGS optimize_group_by_function_keys = 0;

SELECT toString(number) AS v, number, count()
FROM numbers(3)
GROUP BY v, number WITH TOTALS
ORDER BY number
SETTINGS optimize_group_by_function_keys = 1;

-- GROUPING SETS: dropping the function-of-key must not change the non-member set row default.
SELECT toString(number) AS v, number, count()
FROM numbers(3)
GROUP BY GROUPING SETS ((v, number), (number))
ORDER BY number, v, GROUPING(v)
SETTINGS optimize_group_by_function_keys = 0;

SELECT toString(number) AS v, number, count()
FROM numbers(3)
GROUP BY GROUPING SETS ((v, number), (number))
ORDER BY number, v, GROUPING(v)
SETTINGS optimize_group_by_function_keys = 1;

-- The optimization still applies (and results are unchanged) for plain GROUP BY.
SELECT toString(number) AS v, number
FROM numbers(3)
GROUP BY v, number
ORDER BY number
SETTINGS optimize_group_by_function_keys = 0;

SELECT toString(number) AS v, number
FROM numbers(3)
GROUP BY v, number
ORDER BY number
SETTINGS optimize_group_by_function_keys = 1;

-- The same invariants must hold on the legacy AST optimizer (enable_analyzer = 0), which runs
-- optimizeGroupBy (optimize_injective_functions_in_group_by) and optimizeGroupByFunctionKeys
-- (optimize_group_by_function_keys) in src/Interpreters/TreeOptimizer.cpp. Both must skip the
-- rewrite under WITH TOTALS / GROUPING SETS so the totals/non-member rows keep the key column
-- default rather than recomputing it from other keys' defaults (#110715).
SELECT toString(number) AS v, count()
FROM numbers(3)
GROUP BY v WITH TOTALS
ORDER BY v
SETTINGS enable_analyzer = 0, optimize_injective_functions_in_group_by = 1;

-- GROUPING SETS for optimizeGroupBy (injective path) on the legacy optimizer: the non-member set
-- row must keep the key column default, not f(default_of_argument).
SELECT materialize(3) AS x
FROM numbers(10)
GROUP BY GROUPING SETS (('str'), (materialize(3)))
ORDER BY x
SETTINGS enable_analyzer = 0, optimize_injective_functions_in_group_by = 1;

SELECT toString(number) AS v, number, count()
FROM numbers(3)
GROUP BY v, number WITH TOTALS
ORDER BY number
SETTINGS enable_analyzer = 0, optimize_group_by_function_keys = 1;

SELECT toString(number) AS v, number, count()
FROM numbers(3)
GROUP BY GROUPING SETS ((v, number), (number))
ORDER BY number, v, GROUPING(v)
SETTINGS enable_analyzer = 0, optimize_group_by_function_keys = 1;

-- The optimizations still apply (results unchanged) for plain GROUP BY on the legacy path.
SELECT toString(number) AS v, number
FROM numbers(3)
GROUP BY v, number
ORDER BY number
SETTINGS enable_analyzer = 0, optimize_injective_functions_in_group_by = 1, optimize_group_by_function_keys = 1;

-- optimize_aggregators_of_group_by_keys on the legacy path: min/max/any of a GROUP BY key is
-- eliminated to the bare key. Under GROUPING SETS the eliminated aggregate must not be dropped for
-- sets that do not contain the key, otherwise the non-member rows compute it from the key's default
-- instead of the real aggregate (#110715). max(b) on the (a) set must stay 50 / 100, not 0.
SELECT a, max(b) AS mb
FROM values('a UInt64, b UInt64', (1, 10), (1, 50), (2, 60), (2, 100))
GROUP BY GROUPING SETS ((a, b), (a))
ORDER BY a, mb, GROUPING(b)
SETTINGS enable_analyzer = 0, optimize_aggregators_of_group_by_keys = 0;

SELECT a, max(b) AS mb
FROM values('a UInt64, b UInt64', (1, 10), (1, 50), (2, 60), (2, 100))
GROUP BY GROUPING SETS ((a, b), (a))
ORDER BY a, mb, GROUPING(b)
SETTINGS enable_analyzer = 0, optimize_aggregators_of_group_by_keys = 1;
