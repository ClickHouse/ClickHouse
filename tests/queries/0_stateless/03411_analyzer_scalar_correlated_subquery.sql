set enable_analyzer = 1;
set allow_experimental_correlated_subqueries = 1;

EXPLAIN QUERY TREE
SELECT *
FROM numbers(2)
WHERE (SELECT count() FROM system.one WHERE number = 2) is NULL;

-- An empty correlated group makes count() return its empty-input value 0 (not NULL), so `is NULL` is false
-- for every outer row and the query returns nothing. See issue #111615.
SELECT *
FROM numbers(2)
WHERE (SELECT count() FROM system.one WHERE number = 2) is NULL
ORDER BY all;
