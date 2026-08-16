SELECT
    'inner',
    count(),
    sum(l.v),
    sum(r.v)
FROM
(
    SELECT intDiv(number, 2) AS k, number AS v
    FROM numbers(6)
) AS l
ALL INNER JOIN
(
    SELECT intDiv(number, 2) AS k, number + 10 AS v
    FROM numbers(6)
) AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, max_threads = 1;

SELECT
    'right',
    count(),
    sum(ifNull(l.v, 0)),
    sum(r.v)
FROM
(
    SELECT intDiv(number, 2) AS k, number AS v
    FROM numbers(6)
) AS l
ALL RIGHT JOIN
(
    SELECT intDiv(number, 2) AS k, number + 10 AS v
    FROM numbers(8)
) AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, max_threads = 1;

SELECT
    'full',
    count(),
    sum(ifNull(l.v, 0)),
    sum(ifNull(r.v, 0))
FROM
(
    SELECT if(number < 6, intDiv(number, 2), 4) AS k, number AS v
    FROM numbers(8)
) AS l
ALL FULL JOIN
(
    SELECT intDiv(number, 2) AS k, number + 10 AS v
    FROM numbers(8)
) AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, max_threads = 1;

SELECT
    'packed-full',
    count(),
    sum(ifNull(l.v, 0)),
    sum(ifNull(r.v, 0))
FROM
(
    WITH toUInt64(intDiv(number, 2)) AS g
    SELECT
        g AS k1,
        g * 3 + 1 AS k2,
        g * 5 + 2 AS k3,
        g * 7 + 3 AS k4,
        number AS v
    FROM numbers(10)
) AS l
ALL FULL JOIN
(
    WITH toUInt64(if(number < 6, intDiv(number, 2), 5)) AS g
    SELECT
        g AS k1,
        g * 3 + 1 AS k2,
        g * 5 + 2 AS k3,
        g * 7 + 3 AS k4,
        number + 10 AS v
    FROM numbers(8)
) AS r ON l.k1 = r.k1 AND l.k2 = r.k2 AND l.k3 = r.k3 AND l.k4 = r.k4
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, max_threads = 1;
