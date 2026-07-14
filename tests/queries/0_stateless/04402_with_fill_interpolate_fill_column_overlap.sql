-- Regression test for the "Invalid number of rows in Chunk" logical error in FillingTransform
-- when an INTERPOLATE target aliases a WITH FILL column (old analyzer only).
-- https://github.com/ClickHouse/ClickHouse/pull/101099

SET enable_analyzer = 0;

-- INTERPOLATE target that aliases the fill column resolves to the same header position as the
-- fill column, so a gap-fill row would be inserted twice into that column. This must be rejected.
SELECT * FROM (SELECT 1 AS a0, a0 AS a4 ORDER BY a0 WITH FILL INTERPOLATE (a4)); -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- The new analyzer materializes a separate interpolate column, so the same query is valid there.
SELECT * FROM (SELECT 1 AS a0, a0 AS a4 ORDER BY a0 WITH FILL INTERPOLATE (a4)) SETTINGS enable_analyzer = 1;

-- INTERPOLATE on a column that is not a fill column stays valid on the old analyzer.
SELECT n, m FROM (SELECT number AS n, number * 10 AS m FROM numbers(3) WHERE number IN (0, 2))
ORDER BY n WITH FILL INTERPOLATE (m AS m + 1);
