-- Test SEMI/ANTI JOIN column access restrictions with SQL standard semantics.
-- When semi_join_compatibility or anti_join_compatibility settings are enabled,
-- only columns from the preserved side are accessible in:
-- 1. SELECT clause (SELECT *, qualified matchers like t2.*)
-- 2. WHERE clause
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
