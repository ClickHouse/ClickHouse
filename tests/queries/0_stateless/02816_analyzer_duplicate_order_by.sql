set allow_experimental_analyzer = 1;

set optimize_duplicate_order_by_and_distinct = 1;

EXPLAIN QUERY TREE SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT *
        FROM numbers(3)
        ORDER BY number
    )
    ORDER BY number
)
ORDER BY number;

set optimize_duplicate_order_by_and_distinct = 0;

EXPLAIN QUERY TREE SELECT *
FROM
(
   SELECT *
   FROM
   (
       SELECT *
       FROM numbers(3)
       ORDER BY number
   )
   ORDER BY number
)
ORDER BY number;

set optimize_duplicate_order_by_and_distinct = 1;

EXPLAIN QUERY TREE SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT number % 2
        AS number
        FROM numbers(3)
        ORDER BY number
    )
    ORDER BY number
)
ORDER BY number;

set optimize_duplicate_order_by_and_distinct = 0;

EXPLAIN QUERY TREE SELECT *
FROM
(
    SELECT *
    FROM
    (
        SELECT number % 2
        AS number
        FROM numbers(3)
        ORDER BY number
    )
    ORDER BY number
)
ORDER BY number;
