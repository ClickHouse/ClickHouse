-- The old analyzer's AST predicate pushdown must not push a predicate into a subquery
-- whose SELECT list contains `untuple`: the names `untuple` produces appear in that
-- subquery's output block but cannot be referenced inside it.
-- Each row is run with the pushdown both enabled and disabled, so the reference itself
-- proves the two arms agree. `enable_optimize_predicate_expression` is randomized by the
-- test runner, so it is pinned per statement.

SELECT '-- 1 untuple(arrayJoin(map)), three levels';
SELECT t.keys AS label
FROM (SELECT untuple(arrayJoin(m)) AS t
      FROM (SELECT map('a', 1, 'b', 0) AS m) AS mt) AS tt
WHERE t.values > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT t.keys AS label
FROM (SELECT untuple(arrayJoin(m)) AS t
      FROM (SELECT map('a', 1, 'b', 0) AS m) AS mt) AS tt
WHERE t.values > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT t.keys AS label
FROM (SELECT untuple(arrayJoin(m)) AS t
      FROM (SELECT map('a', 1, 'b', 0) AS m) AS mt) AS tt
WHERE t.values > 0
SETTINGS enable_analyzer = 1;

SELECT '-- 2 untuple of a named tuple, no arrayJoin';
SELECT u.k AS label
FROM (SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1)) AS tt
WHERE u.v > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT u.k AS label
FROM (SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1)) AS tt
WHERE u.v > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 3 untuple in a UNION ALL branch';
SELECT u.k AS label FROM (
    SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1)
    UNION ALL
    SELECT untuple(CAST(('y', 7), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1)
) AS tt
WHERE u.v > 0
ORDER BY label
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT u.k AS label FROM (
    SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1)
    UNION ALL
    SELECT untuple(CAST(('y', 7), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1)
) AS tt
WHERE u.v > 0
ORDER BY label
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 4 three levels with re-aliasing';
SELECT w FROM (
    SELECT v AS w FROM (
        SELECT u.v AS v FROM (
            SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1)
        ) AS a
    ) AS b
) AS c
WHERE w > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT w FROM (
    SELECT v AS w FROM (
        SELECT u.v AS v FROM (
            SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1)
        ) AS a
    ) AS b
) AS c
WHERE w > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 5 two untuples, predicate on the second';
SELECT p.a AS l1, q.c AS l2
FROM (SELECT untuple(CAST(('x', 1), 'Tuple(a String, b UInt8)')) AS p,
             untuple(CAST(('y', 2), 'Tuple(c String, d UInt8)')) AS q
      FROM numbers(1)) AS tt
WHERE q.d > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT p.a AS l1, q.c AS l2
FROM (SELECT untuple(CAST(('x', 1), 'Tuple(a String, b UInt8)')) AS p,
             untuple(CAST(('y', 2), 'Tuple(c String, d UInt8)')) AS q
      FROM numbers(1)) AS tt
WHERE q.d > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 6 control: arrayJoin without untuple still pushes';
SELECT v FROM (SELECT arrayJoin([1, 0]) AS v) AS tt WHERE v > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT v FROM (SELECT arrayJoin([1, 0]) AS v) AS tt WHERE v > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 7 control: tuple column with element access still pushes';
SELECT t.1 AS a, t.2 AS b FROM (SELECT tuple(2, 3) AS t FROM numbers(1)) AS tt WHERE t.2 > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT t.1 AS a, t.2 AS b FROM (SELECT tuple(2, 3) AS t FROM numbers(1)) AS tt WHERE t.2 > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 8 control: a column literally named `u.v` still pushes';
SELECT `u.v` FROM (SELECT 5 AS `u.v` FROM numbers(1)) AS tt WHERE `u.v` > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT `u.v` FROM (SELECT 5 AS `u.v` FROM numbers(1)) AS tt WHERE `u.v` > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 9 the coverage the barrier trades away: predicate on a sibling ordinary column';
SELECT u.k AS label, x FROM (
    SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u, number AS x FROM numbers(5)
) AS tt
WHERE x > 2
ORDER BY x
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT u.k AS label, x FROM (
    SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u, number AS x FROM numbers(5)
) AS tt
WHERE x > 2
ORDER BY x
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 10 untuple of an aggregate tuple under GROUP BY';
SELECT u.mn, u.cnt FROM (
    SELECT untuple(CAST((min(number), count()), 'Tuple(mn UInt64, cnt UInt64)')) AS u, intDiv(number, 3) AS g
    FROM numbers(9) GROUP BY g
) AS tt
WHERE g = 1
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT u.mn, u.cnt FROM (
    SELECT untuple(CAST((min(number), count()), 'Tuple(mn UInt64, cnt UInt64)')) AS u, intDiv(number, 3) AS g
    FROM numbers(9) GROUP BY g
) AS tt
WHERE g = 1
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 11 untuple inside a CTE consulted by the same barrier';
WITH c AS (SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1))
SELECT u.k AS label FROM (SELECT * FROM c) AS tt WHERE u.v > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

WITH c AS (SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u FROM numbers(1))
SELECT u.k AS label FROM (SELECT * FROM c) AS tt WHERE u.v > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 12 unaliased untuple: the outputs are named after the generated tupleElement calls';
SELECT 1 AS ok FROM (SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) FROM numbers(1)) AS tt
WHERE `tupleElement(CAST(('x', 5), 'Tuple(k String, v UInt8)'), 2)` > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT 1 AS ok FROM (SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) FROM numbers(1)) AS tt
WHERE `tupleElement(CAST(('x', 5), 'Tuple(k String, v UInt8)'), 2)` > 0
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

-- The rows above compare results only, so they would all stay green if the pushdown became a
-- global no-op. The two below read the index condition instead, which is a direct observation
-- of whether the AST rewrite added its conjunct: pushing produces the duplicate
-- `and((x in [5, 5]), (x in [5, 5]))`, because the plan-level filter pushdown supplies the same
-- condition anyway.
DROP TABLE IF EXISTS t_04812;
CREATE TABLE t_04812 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04812 SELECT number FROM numbers(1000);

SELECT '-- 13 positive oracle: a subquery without untuple still receives the pushed predicate';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT x FROM (SELECT x FROM t_04812) AS tt WHERE x = 5
) WHERE explain LIKE '%Condition:%'
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT x FROM (SELECT x FROM t_04812) AS tt WHERE x = 5
) WHERE explain LIKE '%Condition:%'
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

SELECT '-- 14 the barrier is blanket: an untuple sibling also stops receiving the pushed predicate';
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT u.k, x FROM (
        SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u, x FROM t_04812
    ) AS tt WHERE x = 5
) WHERE explain LIKE '%Condition:%'
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1;

SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT u.k, x FROM (
        SELECT untuple(CAST(('x', 5), 'Tuple(k String, v UInt8)')) AS u, x FROM t_04812
    ) AS tt WHERE x = 5
) WHERE explain LIKE '%Condition:%'
SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0;

DROP TABLE t_04812;
