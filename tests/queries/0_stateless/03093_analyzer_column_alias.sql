-- https://github.com/ClickHouse/ClickHouse/issues/26674
SET enable_analyzer = true;

SELECT
    Carrier,
    sum(toFloat64(C3)) AS C1,
    sum(toFloat64(C1)) AS C2,
    sum(toFloat64(C2)) AS C3
FROM
    (
        SELECT
            1 AS Carrier,
            count(CAST(1, 'Nullable(Int32)')) AS C1,
            max(number) AS C2,
            min(number) AS C3
        FROM numbers(10)
        GROUP BY Carrier
    ) AS ITBL
GROUP BY Carrier
LIMIT 1000001
SETTINGS prefer_column_name_to_alias=1;
