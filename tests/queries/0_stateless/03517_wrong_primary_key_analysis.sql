-- The query `SELECT *, day + 365 AS day` triggers an unrelated analyzer bug
-- (`MULTIPLE_EXPRESSIONS_FOR_ALIAS`, issue #74324) when run with parallel
-- replicas. This test is for #48881 (wrong primary key analysis), so disable
-- parallel replicas explicitly to keep the test deterministic. Remove this
-- guard once #74324 is fixed.
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

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
