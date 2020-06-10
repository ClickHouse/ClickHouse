set enable_debug_queries = 1;
set optimize_duplicate_order_by_and_distinct = 1;

analyze SELECT DISTINCT *
FROM
(
    SELECT DISTINCT *
    FROM
    (
        SELECT DISTINCT *
        FROM numbers(1)
        ORDER BY number
    )
    ORDER BY number
)
ORDER BY number;

set optimize_duplicate_order_by_and_distinct = 0;

analyze SELECT DISTINCT *
FROM
    (
     SELECT DISTINCT *
     FROM
         (
          SELECT DISTINCT *
          FROM numbers(1)
          ORDER BY number
             )
     ORDER BY number
        )
ORDER BY number;
