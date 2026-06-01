-- The query `SELECT *, day + 365 AS day` redefines the alias `day` over a
-- column of the same name. Under parallel replicas the remote replica's
-- analyzer sees two bodies (`day` and `day + 365`) for the alias `day` and
-- throws `MULTIPLE_EXPRESSIONS_FOR_ALIAS` (issue #74324). The fix #103806
-- covered other shapes of that issue but not this self-shadowing alias over
-- `SELECT *`, which still reproduces (verified locally against master after
-- #103806). This test is for #48881 (wrong primary key analysis), so disable
-- parallel replicas explicitly to keep it deterministic regardless of the
-- `ParallelReplicas` CI suite. Remove this guard once the analyzer handles
-- this shape.
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
