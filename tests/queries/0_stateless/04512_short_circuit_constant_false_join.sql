-- Tests the query-plan pass that short-circuits a JOIN with a constant-false ON condition
-- (or an already-empty input). Issue #110225: the non-contributing side must not be read.

SET enable_analyzer = 1;

-- Plan checks: assert the optimization fires (ReadNothing replaces the non-contributing side or
-- the whole join). Use the robust `SELECT ... FROM (EXPLAIN ...) WHERE explain ILIKE ...` form.

SELECT 'INNER JOIN ON false -> whole join is an empty source';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    INNER JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'LEFT JOIN ON false -> right (null) side is an empty source';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    LEFT JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'RIGHT JOIN ON false -> left (null) side is an empty source';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    RIGHT JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'Constant-false from predicate folding (a.t = ''A'' AND a.t = ''B'')';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x, toString(number) AS t FROM numbers(10)) a
    LEFT JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y AND a.t = 'A' AND a.t = 'B'
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'A true ON condition is NOT short-circuited';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number AS x FROM numbers(10)) a
    LEFT JOIN (SELECT number AS y FROM numbers(100)) b ON a.x = b.y
) WHERE explain ILIKE '%ReadNothing%';

-- Result checks: short-circuit must not change results (compare against the pass disabled).

SELECT 'Results are unchanged (each pair prints one line = optimized result)';
SELECT 'INNER', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    INNER JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x, b.y;
SELECT 'LEFT', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    LEFT JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x, b.y;
SELECT 'RIGHT', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    RIGHT JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x, b.y;
SELECT 'FULL', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    FULL JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x, b.y;

SELECT 'LEFT join_use_nulls', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    LEFT JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2
    ORDER BY a.x, b.y SETTINGS join_use_nulls = 1;
SELECT 'FULL join_use_nulls', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    FULL JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2
    ORDER BY a.x, b.y SETTINGS join_use_nulls = 1;

SELECT 'LEFT SEMI', a.x FROM (SELECT number AS x FROM numbers(5)) a
    LEFT SEMI JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x;
SELECT 'LEFT ANTI', a.x FROM (SELECT number AS x FROM numbers(5)) a
    LEFT ANTI JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y AND 1 = 2 ORDER BY a.x;

-- Empty input propagation: an input that is already an empty source (WHERE false) collapses too.
SELECT 'INNER with empty left input';
SELECT a.x, b.y FROM (SELECT number AS x FROM numbers(5) WHERE 1 = 2) a
    INNER JOIN (SELECT number AS y FROM numbers(3)) b ON a.x = b.y ORDER BY a.x, b.y;
SELECT 'LEFT with empty right input', a.x, b.y FROM (SELECT number AS x FROM numbers(5)) a
    LEFT JOIN (SELECT number AS y FROM numbers(3) WHERE 1 = 2) b ON a.x = b.y ORDER BY a.x, b.y;
