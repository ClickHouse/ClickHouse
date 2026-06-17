-- With group_by_use_nulls, a column from one self-join side that is not a GROUP BY key must not be
-- silently replaced with the Nullable clone of a structurally-equal key from the other side: the
-- two sides are distinct column source instances, so r.number is not grouped here and must be
-- reported as not aggregated rather than misbound.
SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT r.number
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY l.number WITH ROLLUP; -- { serverError NOT_AN_AGGREGATE }

-- The grouped column itself resolves and becomes Nullable for the ROLLUP total row.
SELECT l.number
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY l.number WITH ROLLUP
ORDER BY l.number;
