--https://github.com/ClickHouse/ClickHouse/issues/47366
SELECT
    id % 255,
    toTypeName(d.id)
FROM
(
    SELECT
        toLowCardinality(1048577) AS id,
        toLowCardinality(9223372036854775807) AS value
    GROUP BY
        GROUPING SETS (
        (toLowCardinality(1024)),
        (id % 10.0001),
        ((id % 2147483646) != -9223372036854775807),
        ((id % -1) != 255))
    ) AS a
    SEMI LEFT JOIN
(
    SELECT toLowCardinality(9223372036854775807) AS id
    WHERE (id % 2147483646) != NULL
) AS d USING (id)
SETTINGS join_use_nulls=1;
