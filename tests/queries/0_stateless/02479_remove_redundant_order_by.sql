SET allow_experimental_analyzer=1;

SELECT '-- Disable optimize_duplicate_order_by_and_distinct';
SET optimize_duplicate_order_by_and_distinct=0;

SELECT '-- ORDER BY(s) in subqueries are untouched';
EXPLAIN QUERY TREE run_passes = 1
SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
)
ORDER BY number ASC;

SELECT '-- Enable optimize_duplicate_order_by_and_distinct';
SET optimize_duplicate_order_by_and_distinct=1;

SELECT '-- ORDER BY eliminates ORDER BY(s) in subqueries';
EXPLAIN QUERY TREE run_passes = 1
SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
)
ORDER BY number ASC;

SELECT '-- GROUP BY eliminates ORDER BY in _all_ subqueries';
EXPLAIN QUERY TREE run_passes = 1
SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
)
GROUP BY number;

SELECT '-- GROUP BY with aggregation function which does NOT depend on order -> eliminate ORDER BY(s) in _all_ subqueries';
EXPLAIN QUERY TREE run_passes = 1
SELECT sum(number)
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
)
GROUP BY number;

SELECT '-- GROUP BY with aggregation function which depends on order -> keep ORDER BY in first subquery, and eliminate in second subquery';
EXPLAIN QUERY TREE run_passes = 1
SELECT any(number)
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    ORDER BY number DESC
)
GROUP BY number;

SELECT '-- check that optimization is applied recursively to subqueries as well';
SELECT '-- GROUP BY with aggregation function which does NOT depend on order -> eliminate ORDER BY in most inner subquery here';
EXPLAIN QUERY TREE run_passes = 1
SELECT a
FROM
(
    SELECT sum(number) AS a
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    GROUP BY number
)
ORDER BY a ASC;

SELECT '-- GROUP BY with aggregation function which depends on order -> eliminate ORDER BY in most inner subquery since the order will be changed by ORDER BY in main query';
EXPLAIN QUERY TREE run_passes = 1
SELECT a
FROM
(
    SELECT any(number) AS a
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number ASC
    )
    GROUP BY number
)
ORDER BY a ASC;

SELECT '-- Check that optimization works for subqueries as well, - main query have nor ORDER BY nor GROUP BY';
EXPLAIN QUERY TREE run_passes = 1
SELECT a
FROM
(
    SELECT any(number) AS a
    FROM
    (
        SELECT *
        FROM
        (
            SELECT *
            FROM numbers(3)
            ORDER BY number DESC
        )
        ORDER BY number ASC
    )
    GROUP BY number
)
WHERE a > 0;

-- TODO: add tests with LIMIT BY
