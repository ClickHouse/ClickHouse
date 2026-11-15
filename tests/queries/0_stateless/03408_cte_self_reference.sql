-- Tags: no-tsan
SET enable_analyzer = 1;

WITH `cte1` AS (
    SELECT *
    FROM (
        WITH `cte2` AS (
            SELECT
                id,
                key,
                values,
            FROM cte1
            LEFT ARRAY JOIN
                mapKeys(key_values) AS key,
                mapValues(key_values) AS values
        )
        SELECT
            id,
            key,
            value
        FROM cte2 LEFT ARRAY JOIN values AS value
    )
)
SELECT * FROM `cte1` -- { serverError UNKNOWN_TABLE }
