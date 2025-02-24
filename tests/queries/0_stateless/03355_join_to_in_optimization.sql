
EXPLAIN actions = 1, optimize = 1, header = 1
SELECT a.number
FROM numbers_mt(1000000000) AS a,
(
    SELECT number * 13 AS x
    FROM numbers_mt(1000)
) AS b
WHERE a.number = b.x
SETTINGS query_plan_use_new_logical_join_step = 1;

EXPLAIN actions = 1, optimize = 1, header = 1
SELECT
    l.a,
    l.b,
    l.c
FROM
(
    SELECT
        number AS a,
        intDiv(number, 3) AS b,
        number + 1 AS c
    FROM numbers(10)
) AS l
WHERE (l.a, l.b) IN (
    SELECT
        number AS a,
        intDiv(number, 2) AS b
    FROM numbers(10)
)
SETTINGS query_plan_use_new_logical_join_step = 1;