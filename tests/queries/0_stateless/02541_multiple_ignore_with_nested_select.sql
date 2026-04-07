SELECT DISTINCT *
FROM
    (
        SELECT DISTINCT *
        FROM
            (
                SELECT DISTINCT
                    0.5,
                    number % 65536 AS number
                FROM numbers(2)
                ORDER BY
                    ignore(ignore(-1, 10.0001)) DESC NULLS LAST,
                    ignore(2147483648) DESC NULLS FIRST,
                    ignore(255, 0.0001) ASC,
                    number ASC
            )
        ORDER BY number ASC NULLS FIRST
    )
WHERE ignore(2147483648)
ORDER BY number DESC