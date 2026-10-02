-- Named windows and window expressions: scoping, references between windows, and expressions that are not allowed.

SELECT '-- Unknown identifiers in the window definition or in the argument';
SELECT count() OVER (PARTITION BY nope) FROM numbers(3); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() OVER (ORDER BY nope) FROM numbers(3); -- { serverError UNKNOWN_IDENTIFIER }
SELECT sum(nope) OVER () FROM numbers(3); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() OVER w FROM numbers(3) WINDOW w AS (PARTITION BY nope); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() OVER w FROM numbers(3) WINDOW w AS (ORDER BY number, nope); -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- A window name is visible only in the query that defines it';
-- The outer definitions below are valid in both scopes: the inner query and the CTE expose number, and the
-- same definitions work when the window is used in the outer query. Only the reference from the inner scope fails.
SELECT number, count() OVER w AS outer_count FROM (SELECT number FROM numbers(3)) WINDOW w AS (ORDER BY number) ORDER BY number;
WITH cte AS (SELECT number FROM numbers(3)) SELECT number, count() OVER w AS outer_count FROM cte WINDOW w AS (ORDER BY number) ORDER BY number;
SELECT count() OVER w FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT * FROM (SELECT number, count() OVER w FROM numbers(3)) WINDOW w AS (ORDER BY number); -- { serverError BAD_ARGUMENTS }
WITH cte AS (SELECT number, count() OVER w AS c FROM numbers(3)) SELECT number, c FROM cte WINDOW w AS (ORDER BY number); -- { serverError BAD_ARGUMENTS }

SELECT '-- The same window name may be defined independently in sibling subqueries and in the outer query';
SELECT a.n, a.c AS by_parity, b.c AS by_third, count() OVER w AS running
FROM (SELECT number AS n, count() OVER w AS c FROM numbers(6) WINDOW w AS (PARTITION BY number % 2)) AS a
JOIN (SELECT number AS n, count() OVER w AS c FROM numbers(6) WINDOW w AS (PARTITION BY number % 3)) AS b USING (n)
WINDOW w AS (ORDER BY n)
ORDER BY n;

SELECT '-- A named window defined inside a CTE body';
WITH cte AS (SELECT number AS n, sum(number) OVER w AS s FROM numbers(5) WINDOW w AS (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW))
SELECT n, s FROM cte ORDER BY n;

SELECT '-- Windows must be defined before another window refers to them';
SELECT count() OVER w FROM numbers(3) WINDOW w AS (base ORDER BY number), base AS (PARTITION BY number % 2); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER w FROM numbers(3) WINDOW w AS (w); -- { serverError BAD_ARGUMENTS }
SELECT count() OVER (w2 ROWS UNBOUNDED PRECEDING) FROM numbers(3) WINDOW w1 AS (ORDER BY number); -- { serverError BAD_ARGUMENTS }

SELECT '-- A child window adds a frame to a parent window';
SELECT number, count() OVER (w ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rows_frame FROM numbers(5) WINDOW w AS (ORDER BY number) ORDER BY number;
SELECT number,
    count() OVER (w RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS range_frame,
    count() OVER (w GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS groups_frame
FROM numbers(5) WINDOW w AS (ORDER BY intDiv(number, 2)) ORDER BY number;

SELECT '-- A child window adds ORDER BY to a parent with PARTITION BY only, and a grandchild adds the frame';
SELECT number, count() OVER w1 AS partition_count, sum(number) OVER w2 AS running_sum, sum(number) OVER w3 AS pair_sum, sum(number) OVER (w2 ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_pair_sum
FROM numbers(6)
WINDOW w1 AS (PARTITION BY number % 2), w2 AS (w1 ORDER BY number), w3 AS (w2 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY number;

SELECT '-- Repeating a PARTITION BY expression does not change the partitions';
SELECT number, count() OVER (PARTITION BY number % 2, number % 2) AS c, sum(number) OVER (PARTITION BY number % 2, number % 2, number % 2 ORDER BY number) AS s FROM numbers(4) ORDER BY number;
SELECT countIf(a != b), countIf(c != d), count()
FROM
(
    SELECT
        count() OVER (PARTITION BY number % 5, number % 5) AS a,
        count() OVER (PARTITION BY number % 5) AS b,
        sum(number) OVER (PARTITION BY number % 5, number % 5 ORDER BY number) AS c,
        sum(number) OVER (PARTITION BY number % 5 ORDER BY number) AS d
    FROM numbers(1000)
    SETTINGS max_block_size = 32
);

SELECT '-- A window function cannot be nested in another window function, but two window functions may be combined in an expression';
SELECT sum(count() OVER (ORDER BY number)) OVER (ORDER BY number) FROM numbers(3); -- { serverError ILLEGAL_AGGREGATION }
SELECT lead(row_number() OVER (ORDER BY number)) OVER (ORDER BY number) FROM numbers(3); -- { serverError ILLEGAL_AGGREGATION }
SELECT count() OVER (ORDER BY number) + row_number() OVER (ORDER BY number DESC) AS s FROM numbers(3) ORDER BY s;

SELECT '-- Windows over the result of UNION ALL and of GROUP BY WITH ROLLUP';
SELECT n, count() OVER (ORDER BY n) AS c FROM (SELECT number AS n FROM numbers(3) UNION ALL SELECT number FROM numbers(3)) ORDER BY n;
SELECT k, c, count() OVER (ORDER BY k) AS running FROM (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH ROLLUP) ORDER BY k, c;
