SELECT
    count(),
    countOrNull(),
    sum(x),
    sumOrNull(x)
FROM
(
    SELECT number AS x
    FROM numbers(10)
    WHERE number > 10
);

SELECT
    count(),
    countOrNull(),
    sum(x),
    sumOrNull(x)
FROM
(
    SELECT 1 AS x
    WHERE 0
);
