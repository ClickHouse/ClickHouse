-- https://github.com/ClickHouse/ClickHouse/issues/56503
SET enable_analyzer = 1;

SELECT
    tb1.owner_id AS owner_id,
    type
FROM
(
    SELECT number AS owner_id
    FROM numbers(100)
) AS tb1
CROSS JOIN values('type varchar', 'type1', 'type2', 'type3') AS pt
LEFT JOIN
(
    SELECT tb2.owner_id AS owner_id
    FROM
    (
        SELECT number AS owner_id
        FROM numbers(100)
        GROUP BY owner_id
    ) AS tb2
) AS merged USING (owner_id)
WHERE tb1.owner_id = merged.owner_id
GROUP BY
    tb1.owner_id,
    type
FORMAT `Null`;
