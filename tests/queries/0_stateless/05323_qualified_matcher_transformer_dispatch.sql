-- Three tables exercise `JoinToSubqueryTransformVisitor` through the old AST interpreter.
EXPLAIN AST optimize = 1 SELECT t1.* EXCEPT b REPLACE (a + 10 AS a) APPLY negate FROM (SELECT 1 AS a, 2 AS b) AS t1 CROSS JOIN (SELECT 3 AS c) AS t2 CROSS JOIN (SELECT 4 AS d) AS t3;
EXPLAIN AST optimize = 1 SELECT t1.COLUMNS('^(a|b)$') EXCEPT b REPLACE (a + 10 AS a) APPLY negate FROM (SELECT 1 AS a, 2 AS b) AS t1 CROSS JOIN (SELECT 3 AS c) AS t2 CROSS JOIN (SELECT 4 AS d) AS t3;

-- Qualified `COLUMNS` still supports both forms of `RENAME` in the query analyzer.
SELECT t1.COLUMNS('a') APPLY (x -> x + 10) RENAME a AS renamed_a, t2.c, t3.d FROM (SELECT 1 AS a, 2 AS b) AS t1 CROSS JOIN (SELECT 3 AS c) AS t2 CROSS JOIN (SELECT 4 AS d) AS t3 FORMAT TSVWithNames;
SELECT t1.COLUMNS('^(a|b)$') EXCEPT b REPLACE (a + 10 AS a) RENAME (col -> concat(col, '_renamed')), t2.c, t3.d FROM (SELECT 1 AS a, 2 AS b) AS t1 CROSS JOIN (SELECT 3 AS c) AS t2 CROSS JOIN (SELECT 4 AS d) AS t3 FORMAT TSVWithNames;
