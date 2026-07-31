DROP TABLE IF EXISTS testoffset;

create table testoffset Engine=MergeTree partition by toStartOfMonth(day) order by day as select toDate('2022-01-01')+number as day from system.numbers limit 1000;

SELECT count()
FROM
(
    SELECT *
    FROM
    (
        SELECT
            *,
            day + 365 AS day
        FROM
        (
            SELECT *
            FROM testoffset
        )
    )
)
WHERE (day >= '2023-01-01') AND (day <= '2023-04-17');

SELECT count()
FROM
(
    SELECT
        *,
        day + 365 AS day
    FROM
    (
        SELECT *
        FROM testoffset
    )
)
WHERE (day >= '2023-01-01') AND (day <= '2023-04-17');

DROP TABLE testoffset;

create table testoffset Engine=MergeTree partition by toStartOfMonth(day) order by toString(day) as select toDate('2022-01-01')+number as day from system.numbers limit 1000;

SELECT count()
FROM
(
    SELECT *
    FROM
    (
        SELECT
            *,
            day + 365 AS day
        FROM
        (
            SELECT *
            FROM testoffset
        )
    )
)
WHERE (day >= '2023-01-01') AND (day <= '2023-04-17');

SELECT count()
FROM
(
    SELECT
        *,
        day + 365 AS day
    FROM
    (
        SELECT *
        FROM testoffset
    )
)
WHERE (day >= '2023-01-01') AND (day <= '2023-04-17');

DROP TABLE testoffset;
