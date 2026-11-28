-- https://github.com/ClickHouse/ClickHouse/issues/123965
-- A decorrelated subquery is aggregated by its correlated columns over all outer rows at once,
-- so a group-count limit must not count the groups of other evaluations.

SET allow_experimental_lateral_join = 1;

-- Without GROUP BY: one group per evaluation, the limit can never bind.
SELECT o.number AS x, (SELECT count() FROM numbers(100) AS i WHERE i.number % 10 = o.number) AS c
FROM numbers(10) AS o ORDER BY x
SETTINGS max_rows_to_group_by = 3, max_threads = 1;

SELECT groupArray(c) FROM (
    SELECT o.number AS x, (SELECT count() FROM numbers(100000) AS i WHERE i.number % 10 = o.number) AS c
    FROM numbers(10) AS o ORDER BY x)
SETTINGS max_rows_to_group_by = 3, group_by_overflow_mode = 'break', max_block_size = 100, max_threads = 1;

SELECT groupArray(c) FROM (
    SELECT o.number AS x, (SELECT count() FROM numbers(100000) AS i WHERE intDiv(i.number, 10000) = o.number) AS c
    FROM numbers(10) AS o ORDER BY x)
SETTINGS max_rows_to_group_by = 3, group_by_overflow_mode = 'any', max_block_size = 100, max_threads = 1;

SELECT o.number AS x, EXISTS (SELECT count() AS c FROM numbers(100) AS i WHERE i.number % 10 = o.number HAVING c > 5)
FROM numbers(10) AS o ORDER BY x
SETTINGS max_rows_to_group_by = 3, max_threads = 1;

-- With GROUP BY: `any` / `break` would drop groups of other evaluations.
SELECT o.number AS x, (SELECT count() FROM numbers(100) AS i WHERE i.number % 10 = o.number GROUP BY i.number % 10) AS c
FROM numbers(10) AS o ORDER BY x
SETTINGS max_rows_to_group_by = 3, group_by_overflow_mode = 'any'; -- { serverError NOT_IMPLEMENTED }

SELECT o.number AS x, (SELECT count() FROM numbers(100) AS i WHERE i.number % 10 = o.number GROUP BY i.number % 10) AS c
FROM numbers(10) AS o ORDER BY x
SETTINGS max_rows_to_group_by = 3, group_by_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }

SELECT o.number AS x, EXISTS (SELECT i.number % 10 AS k, count() AS c FROM numbers(100) AS i WHERE i.number % 10 = o.number GROUP BY k HAVING c > 5)
FROM numbers(10) AS o ORDER BY x
SETTINGS max_rows_to_group_by = 3, group_by_overflow_mode = 'any'; -- { serverError NOT_IMPLEMENTED }

SELECT o.number AS x, (SELECT count() FROM numbers(100) AS i WHERE i.number % 10 = o.number GROUP BY i.number % 10) AS c
FROM numbers(10) AS o ORDER BY x
SETTINGS max_rows_to_group_by = 100, group_by_overflow_mode = 'throw';

-- `GROUP BY ... LIMIT` must not keep only the first keys of all evaluations.
SELECT groupArray(c) FROM (
    SELECT o.number AS x,
        (SELECT count() FROM numbers(1000) AS i WHERE intDiv(i.number, 100) = o.number GROUP BY intDiv(i.number, 100) LIMIT 1) AS c
    FROM numbers(10) AS o ORDER BY x)
SETTINGS max_rows_to_group_by = 1000000, optimize_trivial_group_by_limit_query = 1, exact_rows_before_limit = 0, max_threads = 1;

SELECT o.number AS x, s.c
FROM numbers(10) AS o
LEFT JOIN LATERAL (SELECT count() AS c FROM numbers(1000) AS i WHERE intDiv(i.number, 100) = o.number GROUP BY intDiv(i.number, 100) LIMIT 1) AS s ON true
ORDER BY x
SETTINGS max_rows_to_group_by = 1000000, optimize_trivial_group_by_limit_query = 1, exact_rows_before_limit = 0, max_threads = 1;

SELECT groupArray(m) FROM (
    SELECT o.number AS x,
        (SELECT max(c) FROM (SELECT count() AS c FROM numbers(1000) AS i WHERE intDiv(i.number, 100) = o.number GROUP BY intDiv(i.number, 100) LIMIT 1)) AS m
    FROM numbers(10) AS o ORDER BY x)
SETTINGS max_rows_to_group_by = 1000000, optimize_trivial_group_by_limit_query = 1, exact_rows_before_limit = 0, max_threads = 1;
