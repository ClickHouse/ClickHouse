-- Regression test: "Column identifier A is already registered" logical error
-- when `additional_result_filter` is used with UNION queries that share column names.
-- The fake table expression created for the additional filter could collide
-- with column identifiers already registered by the query processing.

SET additional_result_filter = 'A > 0';

SELECT 1 AS A
UNION ALL
SELECT 2 AS A;
