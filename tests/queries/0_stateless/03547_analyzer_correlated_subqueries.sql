SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

SELECT number
FROM numbers(10) AS t
WHERE exists((
    SELECT *
    FROM
    (
        SELECT number * 2 AS number
        FROM
        (
            SELECT number
            FROM numbers(6)
            WHERE (number + 2) < t.number
        )
    )
    WHERE number = t.number
))
ORDER BY number
SETTINGS query_plan_merge_filter_into_join_condition = 0;
