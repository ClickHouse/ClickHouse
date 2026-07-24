-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/49526
-- The `RANK` and `DENSE_RANK` window functions take zero arguments per SQL standard.
-- ClickHouse used to silently accept and ignore arbitrary arguments, which was confusing
-- for users (the visible argument suggested it influenced the ranking, but it did not).
-- Such queries now throw `NUMBER_OF_ARGUMENTS_DOESNT_MATCH`. The legacy permissive
-- behavior is gated behind the `allow_rank_dense_rank_arguments` compatibility setting.

-- 1. Default behavior: passing arguments must throw.

SELECT DENSE_RANK(a) OVER (ORDER BY a) FROM (SELECT 1 AS a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT RANK(a) OVER (ORDER BY a) FROM (SELECT 1 AS a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT DENSE_RANK('x') OVER (ORDER BY a) FROM (SELECT 1 AS a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT RANK('x') OVER (ORDER BY a) FROM (SELECT 1 AS a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT DENSE_RANK(a, 'x', a) OVER (ORDER BY a) FROM (SELECT 1 AS a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT DENSE_RANK([a, 'x', a]) OVER (ORDER BY a) FROM (SELECT 1 AS a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The `dense_rank` alias and case-insensitive `Rank` form behave the same way.
SELECT dense_rank(a) OVER (ORDER BY a) FROM (SELECT 1 AS a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT Rank(a) OVER (ORDER BY a) FROM (SELECT 1 AS a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 2. Reproducer query from issue #49526 must throw.

SELECT id, PRT,
       DENSE_RANK(PRT) OVER (ORDER BY id) AS DR
    FROM (SELECT number AS id, if(number = 3, 0, 1) AS PRT FROM numbers(5)) AS t
    ORDER BY id; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 3. Zero-argument forms still work as before.

SELECT DENSE_RANK() OVER (ORDER BY a) FROM (SELECT 1 AS a);
SELECT RANK() OVER (ORDER BY a) FROM (SELECT 1 AS a);
SELECT dense_rank() OVER (ORDER BY a) FROM (SELECT 1 AS a);

-- 4. Backward-compatibility setting `allow_rank_dense_rank_arguments` restores the
--    legacy behavior of silently ignoring the arguments.

SET allow_rank_dense_rank_arguments = 1;

SELECT DENSE_RANK(a) OVER (ORDER BY a) FROM (SELECT 1 AS a);
SELECT RANK(a) OVER (ORDER BY a) FROM (SELECT 1 AS a);
SELECT DENSE_RANK('x') OVER (ORDER BY a) FROM (SELECT 1 AS a);
SELECT RANK('x') OVER (ORDER BY a) FROM (SELECT 1 AS a);
SELECT DENSE_RANK(a, 'x', a) OVER (ORDER BY a) FROM (SELECT 1 AS a);
SELECT DENSE_RANK([a, 'x', a]) OVER (ORDER BY a) FROM (SELECT 1 AS a);

-- The original reproducer query: ranks fall back to the `OVER (ORDER BY id)`
-- window because the argument was always ignored.
SELECT id, PRT,
       DENSE_RANK(PRT) OVER (ORDER BY id) AS DR,
       RANK(PRT) OVER (ORDER BY id) AS R
    FROM (SELECT number AS id, if(number = 3, 0, 1) AS PRT FROM numbers(5)) AS t
    ORDER BY id;

-- 5. Setting also applies under the new analyzer (default) and the legacy one.

SET enable_analyzer = 0;
SELECT DENSE_RANK(a) OVER (ORDER BY a) FROM (SELECT 1 AS a);
SET enable_analyzer = 1;
SELECT DENSE_RANK(a) OVER (ORDER BY a) FROM (SELECT 1 AS a);
