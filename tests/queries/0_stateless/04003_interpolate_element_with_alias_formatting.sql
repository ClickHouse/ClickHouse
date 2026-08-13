-- Verify that INTERPOLATE with aliased expressions round-trips correctly.
-- The expression in INTERPOLATE must be wrapped in parentheses when it has an alias,
-- otherwise the double AS (col AS expr AS alias) is not parseable.
SELECT formatQuerySingleLine('SELECT 1 ORDER BY a WITH FILL INTERPOLATE (a AS (1 AS b))');
SELECT formatQuerySingleLine('SELECT 1 ORDER BY a WITH FILL INTERPOLATE (a AS (1 AS b), c AS c, d AS (d + 1 AS e))');
