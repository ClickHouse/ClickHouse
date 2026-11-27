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
SETTINGS enable_analyzer = 1;

SELECT '-- 2 untuple of a named tuple, no arrayJoin';


SELECT '-- 3 untuple in a UNION ALL branch';


SELECT '-- 4 three levels with re-aliasing';


SELECT '-- 5 two untuples, predicate on the second';


SELECT '-- 6 control: arrayJoin without untuple still pushes';


SELECT '-- 7 control: tuple column with element access still pushes';


SELECT '-- 8 control: a column literally named `u.v` still pushes';


SELECT '-- 9 the coverage the barrier trades away: predicate on a sibling ordinary column';


SELECT '-- 10 untuple of an aggregate tuple under GROUP BY';


SELECT '-- 11 untuple inside a CTE consulted by the same barrier';


SELECT '-- 12 unaliased untuple: the outputs are named after the generated tupleElement calls';


-- The rows above compare results only, so they would all stay green if the pushdown became a
-- global no-op. The two below read the index condition instead, which is a direct observation
-- of whether the AST rewrite added its conjunct: pushing produces the duplicate
-- `and((x in [5, 5]), (x in [5, 5]))`, because the plan-level filter pushdown supplies the same
-- condition anyway.
DROP TABLE IF EXISTS t_04812;
CREATE TABLE t_04812 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04812 SELECT number FROM numbers(1000);

SELECT '-- 13 positive oracle: a subquery without untuple still receives the pushed predicate';


SELECT '-- 14 the barrier is blanket: an untuple sibling also stops receiving the pushed predicate';


DROP TABLE t_04812;
