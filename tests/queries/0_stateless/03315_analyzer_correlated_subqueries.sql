SET query_plan_pretty_default = 0;
set enable_analyzer = 1;
set allow_experimental_correlated_subqueries = 1;

EXPLAIN QUERY TREE
SELECT *
FROM numbers(2)
WHERE exists((
    SELECT count()
    WHERE number = 2
));

EXPLAIN QUERY TREE
SELECT *
FROM numbers(2)
WHERE exists((
    SELECT
        1,
        dummy,
        1
    WHERE number = 2
));
