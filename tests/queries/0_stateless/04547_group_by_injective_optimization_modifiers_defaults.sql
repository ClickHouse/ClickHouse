-- Regression for #110715 / follow-up to #110721: keep the injective-function GROUP BY optimization
-- (optimize_injective_functions_in_group_by) enabled under GROUP BY modifiers while emitting the correct
-- output default for absent-key rows, instead of recomputing f(column-default). Every result below must be
-- identical with the optimization ON and OFF; the EXPLAIN assertion verifies the optimization still fires.
-- Every setting that affects these results is pinned per query, so the test is parallel-safe and does not
-- rely on any default randomization.

SET enable_analyzer = 1;
SET optimize_injective_functions_in_group_by = 1;
SET group_by_use_nulls = 0;
SET force_grouping_standard_compatibility = 1;

SELECT '-- plain WITH TOTALS: totals row is the String default, not toString(0)';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS ORDER BY s;

SELECT '-- GROUPING SETS with a non-member () set: the () row is the String default';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ())
ORDER BY grouping(toString(number)) DESC, s;

SELECT '-- ROLLUP: the rolled-up () level is the String default';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number))
ORDER BY grouping(toString(number)) DESC, s;

SELECT '-- CUBE: the () level is the String default';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY CUBE(toString(number))
ORDER BY grouping(toString(number)) DESC, s;

SELECT '-- ROLLUP ... WITH TOTALS: subtotal rows via grouping conditional, grand total via totals port';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY ROLLUP(toString(number)) WITH TOTALS
ORDER BY grouping(toString(number)) DESC, s;

SELECT '-- GROUPING SETS with force_grouping_standard_compatibility = 0 (the other grouping() branch)';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY GROUPING SETS ((toString(number)), ())
ORDER BY grouping(toString(number)) DESC, s
SETTINGS force_grouping_standard_compatibility = 0;

SELECT '-- type matrix under plain WITH TOTALS where defaultOf(resultType) != f(default)';
SELECT reinterpretAsString(number) AS s, count() AS c FROM numbers(3)
GROUP BY reinterpretAsString(number) WITH TOTALS ORDER BY s;
SELECT toFixedString(toString(number), 4) AS s, count() AS c FROM numbers(3)
GROUP BY toFixedString(toString(number), 4) WITH TOTALS ORDER BY s;
SELECT toLowCardinality(toString(number)) AS s, count() AS c FROM numbers(3)
GROUP BY toLowCardinality(toString(number)) WITH TOTALS ORDER BY s;

SELECT '-- negative guard (CASE B): user groups by the argument and projects f(argument);';
SELECT '-- here the correct totals value IS toString(0) = 0 and must be unchanged';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY number WITH TOTALS ORDER BY s;

SELECT '-- group_by_use_nulls = 1: optimization is not applied, result stays correct';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS group_by_use_nulls = 1;

SELECT '-- the optimization still fires: the aggregation key is the unwrapped column, not the function';
SELECT trimLeft(explain)
FROM (EXPLAIN PLAN
      SELECT toString(number) AS s, count() FROM numbers(3)
      GROUP BY toString(number) WITH TOTALS)
WHERE explain ILIKE '%Keys:%';

SELECT '-- optimization on == off: same result with the optimization disabled';
SELECT toString(number) AS s, count() AS c FROM numbers(3)
GROUP BY toString(number) WITH TOTALS ORDER BY s
SETTINGS optimize_injective_functions_in_group_by = 0;
