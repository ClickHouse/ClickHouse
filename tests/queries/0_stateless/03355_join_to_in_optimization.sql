
EXPLAIN actions = 1, optimize = 1, header = 1
SELECT a.number
FROM numbers_mt(1000000000) AS a,
(
    SELECT number * 13 AS x
    FROM numbers_mt(1000)
) AS b
WHERE a.number = b.x
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;

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
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;

SELECT deleted
FROM
(
    SELECT
        1 AS deleted,
        'k' AS a,
        'v' AS b
) AS q
ANY INNER JOIN
(
    SELECT
        'k' AS a,
        'v' AS c
) AS s ON q.a = s.a
WHERE deleted AND (b = c);

EXPLAIN optimize = 1
SELECT hostName() AS hostName
FROM system.query_log AS a
INNER JOIN system.processes AS b ON (a.query_id = b.query_id) AND (type = 'QueryStart')
SETTINGS query_plan_use_new_logical_join_step = true, query_plan_convert_join_to_in = true;