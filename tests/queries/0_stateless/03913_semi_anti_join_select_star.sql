-- Test that SELECT * in SEMI/ANTI JOIN follows SQL standard semantics
-- when the corresponding setting is enabled:
-- only returns columns from the preserved side

-- only with new analyzer
SET allow_experimental_analyzer = 1;

-- Test ANTI JOIN setting
SET anti_join_select_star_compatibility = 1;

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
SET semi_join_select_star_compatibility = 1;

-- LEFT SEMI JOIN with true condition: matched rows, only left columns
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON true;

-- LEFT SEMI JOIN with false condition: no rows
SELECT * FROM (SELECT 1 AS a) t1 LEFT SEMI JOIN (SELECT 2 AS b) t2 ON false;

-- RIGHT SEMI JOIN: only right columns
SELECT * FROM (SELECT 1 AS a) t1 RIGHT SEMI JOIN (SELECT 2 AS b) t2 ON true;

-- Explicit column reference still works (t1.*, t2.* are qualified matchers)
SELECT t1.*, t2.* FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false;

-- Default behavior (both settings = 0): returns columns from both sides
SELECT * FROM (SELECT 1 AS a) t1 LEFT ANTI JOIN (SELECT 2 AS b) t2 ON false
SETTINGS anti_join_select_star_compatibility = 0;
