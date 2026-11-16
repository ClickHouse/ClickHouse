-- Regression test: CorrelatedColumnsCollector must handle QUERY/UNION nodes
-- that are not in the scope map (e.g. inside INTERPOLATE expressions).
-- Previously this caused "Cannot find the original scope of the column" exception.

SET allow_experimental_analyzer = 1;

SELECT 1 AS a, a AS b ORDER BY 1 WITH FILL STALENESS 1 INTERPOLATE (a AS b);

SELECT * FROM (
    SELECT 1 AS a0, a0 AS a3, a3 AS a4
    ORDER BY a0 WITH FILL STALENESS a0
    INTERPOLATE (a3 AS a4)
) FORMAT Null;
