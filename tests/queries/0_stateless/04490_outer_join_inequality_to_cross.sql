-- Outer joins with a non-equi (e.g. inequality) ON expression and no equi key are supported the
-- same way as inner joins: a constant join key is added so the hash join enumerates every pair of
-- rows, and the predicate becomes a residual (mixed) join condition. Non-matching rows are
-- NULL-extended (or filled with defaults) as required by the outer join semantics.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t1 VALUES (1, 1), (2, 2), (5, 5);

CREATE TABLE t2 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t2 VALUES (2, 2), (3, 3);

SET join_use_nulls = 1;

-- By default such an outer join is rejected (the error recommends the setting below).
SELECT t1.a, t2.a FROM t1 LEFT JOIN t2 ON t1.c >= t2.c; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SET allow_inequality_join_as_cross_join = 1;

SELECT '-- inner (baseline) --';
SELECT t1.a, t2.a FROM t1 INNER JOIN t2 ON t1.c >= t2.c ORDER BY t1.a, t2.a;
SELECT '-- left --';
SELECT t1.a, t2.a FROM t1 LEFT JOIN t2 ON t1.c >= t2.c ORDER BY t1.a, t2.a;
SELECT '-- right --';
SELECT t1.a, t2.a FROM t1 RIGHT JOIN t2 ON t1.c >= t2.c ORDER BY t1.a, t2.a;
SELECT '-- full --';
SELECT t1.a, t2.a FROM t1 FULL JOIN t2 ON t1.c >= t2.c ORDER BY t1.a, t2.a;

SELECT '-- left, strict less-than --';
SELECT t1.a, t2.a FROM t1 LEFT JOIN t2 ON t1.c < t2.c ORDER BY t1.a, t2.a;
SELECT '-- left, not-equals --';
SELECT t1.a, t2.a FROM t1 LEFT JOIN t2 ON t1.c != t2.c ORDER BY t1.a, t2.a;

SELECT '-- left, join_use_nulls = 0 fills defaults --';
SELECT t1.a, t2.a FROM t1 LEFT JOIN t2 ON t1.c >= t2.c ORDER BY t1.a, t2.a SETTINGS join_use_nulls = 0;

SELECT '-- left, equivalence with cross + anti rewrite (1 = equal) --';
SELECT
    (SELECT arraySort(groupArray((t1.a, coalesce(t2.a, 0)))) FROM t1 LEFT JOIN t2 ON t1.c >= t2.c)
    =
    (
        SELECT arraySort(groupArray((a, coalesce(b, 0)))) FROM (
            SELECT t1.a AS a, t2.a AS b FROM t1 CROSS JOIN t2 WHERE t1.c >= t2.c
            UNION ALL
            SELECT t1.a AS a, NULL AS b FROM t1 WHERE t1.a NOT IN (SELECT t1.a FROM t1 CROSS JOIN t2 WHERE t1.c >= t2.c)
        )
    );

DROP TABLE t1;
DROP TABLE t2;
