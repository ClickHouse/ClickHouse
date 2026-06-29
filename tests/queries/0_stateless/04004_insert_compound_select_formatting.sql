-- Test that INSERT with compound SELECT (INTERSECT/EXCEPT) formats correctly
-- and can be re-parsed. This is a regression test for a bug where the INSERT
-- parser failed on double parentheses like ((SELECT ...) EXCEPT ALL (SELECT ...)).

SELECT formatQuery('INSERT INTO t1 ((SELECT 1) EXCEPT ALL (SELECT 2)) INTERSECT (SELECT 3)');
SELECT formatQuery('INSERT INTO t1 ((SELECT 1) INTERSECT (SELECT 2)) EXCEPT ALL (SELECT 3)');
SELECT formatQuery('INSERT INTO t1 ((SELECT 1) UNION ALL (SELECT 2)) INTERSECT (SELECT 3)');

-- Verify round-trip consistency
SELECT formatQuery(formatQuery('INSERT INTO t1 ((SELECT 1) EXCEPT ALL (SELECT 2)) INTERSECT (SELECT 3)'));

-- Regular INSERT forms should still work
SELECT formatQuery('INSERT INTO t1 (a, b, c) VALUES (1, 2, 3)');
SELECT formatQuery('INSERT INTO t1 (SELECT 1, 2, 3)');
SELECT formatQuery('INSERT INTO t1 SELECT 1, 2, 3');
