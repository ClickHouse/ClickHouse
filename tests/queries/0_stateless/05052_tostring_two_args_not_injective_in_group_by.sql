-- Two-argument toString(value, timezone) is not injective in `value`: with a NULL
-- timezone the result is a constant NULL (Nullable(Nothing)), so GROUP BY over it
-- must collapse to a single group. optimize_injective_functions_in_group_by used to
-- treat toString as injective regardless of arity and rewrite
-- `GROUP BY toString(number, NULL)` into `GROUP BY number`, producing one group per
-- row instead of one.

SET optimize_injective_functions_in_group_by = 1;

-- The grouping key is NULL for every row -> exactly one group.
SELECT count() FROM (SELECT 1 FROM numbers(10) GROUP BY toString(number, NULL));

-- Grouping by an alias of the same expression must agree.
SELECT count() FROM (SELECT toString(number, NULL) AS k FROM numbers(10) GROUP BY k);

-- Single-argument toString stays injective, so this still groups per distinct value.
SELECT count() FROM (SELECT 1 FROM numbers(10) GROUP BY toString(number));

-- The result must not depend on the optimization.
SELECT count() FROM (SELECT 1 FROM numbers(10) GROUP BY toString(number, NULL)) SETTINGS optimize_injective_functions_in_group_by = 0;

-- Same behaviour with the old query analyzer.
SELECT count() FROM (SELECT 1 FROM numbers(10) GROUP BY toString(number, NULL)) SETTINGS allow_experimental_analyzer = 0;
