-- Test SEMI/ANTI JOIN column access restrictions with SQL standard semantics.
-- When semi_join_compatibility or anti_join_compatibility settings are enabled,
-- only columns from the preserved side are accessible in expressions resolved
-- from the joined result, including:
-- 1. SELECT clause (SELECT *, qualified matchers like t2.*)
-- 2. PREWHERE and WHERE clauses
-- 3. GROUP BY, HAVING, and QUALIFY clauses
-- 4. LIMIT BY and ORDER BY clauses
--
-- Preserved side:
-- - LEFT SEMI/ANTI JOIN: left side preserved, right side not accessible
-- - RIGHT SEMI/ANTI JOIN: right side preserved, left side not accessible
--
-- Note: JOIN ON expressions can access both sides regardless of settings.

-- only with new analyzer
SET allow_experimental_analyzer = 1;

-- Test ANTI JOIN setting
SET anti_join_compatibility = 1;
SET semi_join_compatibility = 0;

-- LEFT ANTI JOIN with false condition: all left rows returned, only left columns
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false;

-- LEFT ANTI JOIN with true condition: no rows returned
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON true;

-- Multiple columns on left side
SELECT * FROM (SELECT 1 AS a, 2 AS c) t1 LEFT ANTI JOIN (SELECT 3 AS b, 4 AS d) t2 ON false;

-- RIGHT ANTI JOIN: only right columns
SELECT * FROM (SELECT 1 AS a) t1 RIGHT ANTI JOIN (SELECT 2 AS b) t2 ON false;

-- SEMI JOIN is not affected by anti_join setting: returns columns from both sides
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true;

-- Test SEMI JOIN setting
SET semi_join_compatibility = 1;

-- LEFT SEMI JOIN with true condition: matched rows, only left columns
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true;

-- LEFT SEMI JOIN with false condition: no rows
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON false;

-- RIGHT SEMI JOIN: only right columns
SELECT * FROM (SELECT 1 AS a) t1 RIGHT SEMI JOIN (SELECT 2 AS b) t2 ON true;

-- Explicit column reference: preserved side works, non-preserved side fails
SELECT t1.* FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false;
SELECT t2.* FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false; -- { serverError UNKNOWN_IDENTIFIER }

-- Default behavior (both settings = 0): returns columns from both sides
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false
SETTINGS anti_join_compatibility = 0;

-- Test that non-preserved side columns are not accessible with qualified references
SELECT d.* FROM (SELECT 1 AS id, 2 AS value) AS l SEMI LEFT JOIN (SELECT 1 AS id, 3 AS values) AS d USING id; -- { serverError UNKNOWN_IDENTIFIER }
SELECT l.* FROM (SELECT 1 AS id, 2 AS value) AS l SEMI RIGHT JOIN (SELECT 1 AS id, 3 AS values) AS d USING id; -- { serverError UNKNOWN_IDENTIFIER }
SELECT d.* FROM (SELECT 1 AS id, 2 AS value) AS l ANTI LEFT JOIN (SELECT 2 AS id, 3 AS values) AS d USING id; -- { serverError UNKNOWN_IDENTIFIER }
SELECT l.* FROM (SELECT 2 AS id, 2 AS value) AS l ANTI RIGHT JOIN (SELECT 1 AS id, 3 AS values) AS d USING id; -- { serverError UNKNOWN_IDENTIFIER }

-- Test USING column access
SELECT id FROM (SELECT 1 AS id) AS l SEMI LEFT JOIN (SELECT 1 AS id) AS d USING id;
SELECT l.id FROM (SELECT 1 AS id) AS l SEMI LEFT JOIN (SELECT 1 AS id) AS d USING id;
SELECT d.id FROM (SELECT 1 AS id) AS l SEMI LEFT JOIN (SELECT 1 AS id) AS d USING id; -- { serverError UNKNOWN_IDENTIFIER }

-- Test USING with non-existent columns
SELECT * FROM (SELECT 1 AS other_id) AS l SEMI LEFT JOIN (SELECT 1 AS id) AS d USING (id); -- { serverError UNKNOWN_IDENTIFIER }

-- Test WHERE clause: non-preserved side columns are not accessible
-- LEFT SEMI JOIN: cannot reference right side columns in WHERE
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true WHERE t2.b = 2; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true WHERE t1.a = 1;

-- RIGHT SEMI JOIN: cannot reference left side columns in WHERE
SELECT * FROM (SELECT 1 AS a) t1 RIGHT SEMI JOIN (SELECT 2 AS b) t2 ON true WHERE t1.a = 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM (SELECT 1 AS a) t1 RIGHT SEMI JOIN (SELECT 2 AS b) t2 ON true WHERE t2.b = 2;

-- LEFT ANTI JOIN: cannot reference right side columns in WHERE
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false WHERE t2.b = 2; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false WHERE t1.a = 1;

-- RIGHT ANTI JOIN: cannot reference left side columns in WHERE
SELECT * FROM (SELECT 1 AS a) t1 RIGHT ANTI JOIN (SELECT 2 AS b) t2 ON false WHERE t1.a = 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM (SELECT 1 AS a) t1 RIGHT ANTI JOIN (SELECT 2 AS b) t2 ON false WHERE t2.b = 2;

-- Test nested JOINs: SEMI/ANTI JOIN restrictions apply only within their scope
-- Nested: (t1 LEFT SEMI JOIN t2) LEFT JOIN t3
-- t2 is non-preserved in inner SEMI JOIN, t3 is in outer regular JOIN
SELECT t2.* FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true LEFT JOIN (SELECT 3 AS c) t3 ON true; -- { serverError UNKNOWN_IDENTIFIER }
SELECT t3.* FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true LEFT JOIN (SELECT 3 AS c) t3 ON true;

-- Nested: t1 LEFT JOIN (t2 LEFT SEMI JOIN t3)
-- t3 is non-preserved in SEMI JOIN, t1 is in outer regular JOIN
SELECT t3.* FROM (SELECT 1 AS a) t1 LEFT JOIN (SELECT * FROM (SELECT 2 AS b) t2 LEFT SEMI JOIN (SELECT 3 AS c) t3 ON true) sub ON true; -- { serverError UNKNOWN_IDENTIFIER }
SELECT t1.* FROM (SELECT 1 AS a) t1 LEFT JOIN (SELECT * FROM (SELECT 2 AS b) t2 LEFT SEMI JOIN (SELECT 3 AS c) t3 ON true) sub ON true;

-- Nested: (t1 LEFT SEMI JOIN t2) LEFT JOIN t3
-- In outer JOIN's ON clause, t2 (non-preserved in inner SEMI JOIN) should still be inaccessible
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true LEFT JOIN (SELECT 3 AS c) t3 ON t2.b = t3.c; -- { serverError UNKNOWN_IDENTIFIER }
-- But t1 (preserved in inner SEMI JOIN) should be accessible in outer JOIN's ON clause
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true LEFT JOIN (SELECT 3 AS c) t3 ON t1.a = t3.c;
-- Test that inner SEMI JOIN's own ON expression can still access both sides
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON t1.a = t2.b LEFT JOIN (SELECT 3 AS c) t3 ON t1.a = t3.c;
-- Nested `JOIN` resolution inside a scalar subquery in outer `ON` must not lose outer `JOIN` context
SELECT *
FROM (SELECT 1 AS a) t1
LEFT SEMI JOIN (SELECT 1 AS b) t2
    ON (SELECT count() FROM (SELECT 1 AS c) t3 INNER JOIN (SELECT 1 AS d) t4 ON t3.c = t4.d) > 0
    AND t1.a = t2.b;

-- (t1 LEFT SEMI JOIN t2) LEFT SEMI JOIN t3: outer sees t2 on its preserved left side, but
-- the inner LEFT SEMI JOIN places t2 on its non-preserved right side -- must be denied.
SELECT t2.* FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 1 AS b) t2 ON true LEFT SEMI JOIN (SELECT 1 AS c) t3 ON true; -- { serverError UNKNOWN_IDENTIFIER }
-- t1 is on the preserved left of both joins -- must be allowed.
SELECT t1.* FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 1 AS b) t2 ON true LEFT SEMI JOIN (SELECT 1 AS c) t3 ON true;
-- (t1 LEFT SEMI JOIN t2) RIGHT SEMI JOIN t3: outer RIGHT SEMI makes its entire left subtree
-- (including t1) non-preserved, regardless of the inner join's own preservation.
SELECT t1.* FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 1 AS b) t2 ON true RIGHT SEMI JOIN (SELECT 1 AS c) t3 ON true; -- { serverError UNKNOWN_IDENTIFIER }
-- t3 is on the preserved right of the outer RIGHT SEMI JOIN -- must be allowed.
SELECT t3.* FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 1 AS b) t2 ON true RIGHT SEMI JOIN (SELECT 1 AS c) t3 ON true;
-- (t1 LEFT SEMI JOIN t2) CROSS JOIN t3: t2 remains non-preserved due to the inner LEFT SEMI JOIN
-- and must stay inaccessible even when wrapped by CROSS JOIN at the root.
SELECT t2.* FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 1 AS b) t2 ON true CROSS JOIN (SELECT 1 AS c) t3; -- { serverError UNKNOWN_IDENTIFIER }
-- t1 is preserved by the inner LEFT SEMI JOIN and should remain accessible.
SELECT t1.* FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 1 AS b) t2 ON true CROSS JOIN (SELECT 1 AS c) t3;

-- Additional clause coverage: PREWHERE, GROUP BY, HAVING, QUALIFY, LIMIT BY, and ORDER BY

-- 'PREWHERE';
DROP TABLE IF EXISTS semi_anti_prewhere_left;
DROP TABLE IF EXISTS semi_anti_prewhere_right;
CREATE TABLE semi_anti_prewhere_left (number UInt64) ENGINE = MergeTree ORDER BY number;
CREATE TABLE semi_anti_prewhere_right (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO semi_anti_prewhere_left VALUES (1);
INSERT INTO semi_anti_prewhere_right VALUES (1);
SELECT * FROM semi_anti_prewhere_left AS t1 LEFT SEMI JOIN semi_anti_prewhere_right AS t2 ON true PREWHERE t2.number = 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM semi_anti_prewhere_left AS t1 LEFT SEMI JOIN semi_anti_prewhere_right AS t2 ON true PREWHERE t1.number = 1;
DROP TABLE semi_anti_prewhere_left;
DROP TABLE semi_anti_prewhere_right;

-- 'HAVING';
SELECT t1.a FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true GROUP BY t1.a HAVING t2.b = 2; -- { serverError UNKNOWN_IDENTIFIER }
SELECT t1.a FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true GROUP BY t1.a HAVING t1.a = 1;

-- 'GROUP BY';
SELECT t1.a FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true GROUP BY t2.b; -- { serverError UNKNOWN_IDENTIFIER }
SELECT t1.a FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true GROUP BY t1.a;

-- 'QUALIFY';
SELECT t1.a FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true QUALIFY row_number() OVER () = 1 AND t2.b = 2; -- { serverError UNKNOWN_IDENTIFIER }
SELECT t1.a FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true QUALIFY row_number() OVER () = 1 AND t1.a = 1;

-- 'LIMIT BY';
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false LIMIT 1 BY t2.b; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false LIMIT 1 BY t1.a;

-- 'ORDER BY';
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false ORDER BY t2.b; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false ORDER BY t1.a;
