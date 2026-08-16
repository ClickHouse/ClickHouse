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
