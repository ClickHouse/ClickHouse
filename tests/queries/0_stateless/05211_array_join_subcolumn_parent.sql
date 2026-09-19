-- A subcolumn from a different column must not be treated as a subcolumn of
-- the column being ARRAY JOINed.
SET enable_analyzer = 1;

SELECT a.values AS joined_values, b.values AS untouched_values
FROM
(
    SELECT map('a', 1, 'b', 2) AS a, map('x', 10, 'y', 20) AS b
) AS t
ARRAY JOIN a
ORDER BY joined_values;
