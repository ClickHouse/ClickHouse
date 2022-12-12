set allow_experimental_analyzer=1;
set optimize_duplicate_order_by_and_distinct = 0;

set query_plan_remove_redundant_distinct=0;

EXPLAIN header=1
SELECT DISTINCT *
FROM
(
    SELECT DISTINCT *
    FROM
    (
        SELECT DISTINCT *
        FROM numbers(3)
    )
);

set query_plan_remove_redundant_distinct=1;
EXPLAIN header=1
SELECT DISTINCT *
FROM
(
    SELECT DISTINCT *
    FROM
    (
        SELECT DISTINCT *
        FROM numbers(3)
    )
);
