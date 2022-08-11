SELECT
    (
        SELECT
            1 AS number,
            number
        FROM numbers(1)
    ) AS s,
    (
        SELECT
            1,
            number
        FROM numbers(1)
    ) AS s2
