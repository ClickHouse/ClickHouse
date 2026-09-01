-- A column participating in ORDER BY ... WITH FILL must not also be an INTERPOLATE output.
-- When the fill column was a constant, the old analyzer folded it and the INTERPOLATE target
-- to the same source column, the validation was skipped, and the query aborted with a chunk
-- row-count logical error instead of a clean exception. Found by the AST fuzzer.
-- The fold only happens under the old analyzer, so pin it to exercise the validation deterministically.

SET enable_analyzer = 0;

-- Original AST-fuzzer query shape.
SELECT 1024 AS a, a AS b ORDER BY 1 DESC NULLS FIRST WITH FILL STALENESS -2 INTERPOLATE (`a` AS b); -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- Same conflict referenced by the column alias instead of the positional argument.
SELECT 1024 AS a, a AS b ORDER BY a DESC WITH FILL STALENESS -2 INTERPOLATE (`a` AS b); -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- Minimal reproducer without STALENESS: two columns that fold to the same constant.
SELECT 1 AS a, 1 AS x ORDER BY a WITH FILL FROM 1 TO 5 INTERPOLATE (`x` AS x); -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- Valid queries must keep working: the INTERPOLATE target is a different column.
SELECT 1 AS a, 2 AS x ORDER BY a WITH FILL FROM 1 TO 3 INTERPOLATE (`x` AS x);
SELECT 5 AS a, 100 AS x ORDER BY a WITH FILL FROM 1 TO 3 INTERPOLATE (`x` AS x + 1);
SELECT number AS n, number + 10 AS x FROM numbers(3) ORDER BY n WITH FILL FROM 0 TO 5 INTERPOLATE (`x` AS x + 1);

-- The current analyzer does not fold identical constants onto one source column, so the INTERPOLATE
-- target stays a genuinely distinct output column from the fill key and is interpolated independently.
-- That is why the rejection above is scoped to the legacy old-analyzer fold (where the very same query
-- would otherwise abort with a chunk row-count logical error); under the analyzer the query is valid and
-- returns the interpolated rows. This guards that the fix does not reject distinct projections in general.
SET enable_analyzer = 1;
SELECT 1 AS a, 1 AS x ORDER BY a WITH FILL FROM 1 TO 5 INTERPOLATE (`x` AS x);
