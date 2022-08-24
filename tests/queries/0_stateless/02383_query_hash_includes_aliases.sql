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
    ) AS s2;

-- Check now that function aliases are included in the hash too
-- The 2 subqueries are different as the first column alias is changed and that modifies the second column value
SELECT
    (
        SELECT
                1 + 2 AS number,
                1 + number AS b
        FROM system.numbers
        LIMIT 10, 1
    ),
    (
        SELECT
                1 + 2 AS number2,
                1 + number AS b
        FROM system.numbers
        LIMIT 10, 1
    )
