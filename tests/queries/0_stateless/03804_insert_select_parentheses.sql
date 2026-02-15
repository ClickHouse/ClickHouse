-- Test that INSERT SELECT with parentheses around the SELECT part is supported
-- This was previously rejected by the parser

DROP TABLE IF EXISTS t_insert_select_parens;
CREATE TABLE t_insert_select_parens (x UInt64) ENGINE = Memory;

-- Basic case: INSERT SELECT with parentheses
INSERT INTO t_insert_select_parens (x) (SELECT 1);
SELECT * FROM t_insert_select_parens ORDER BY x;
TRUNCATE TABLE t_insert_select_parens;

-- INSERT SELECT with UNION and parentheses
INSERT INTO t_insert_select_parens (x) (SELECT 2 UNION ALL SELECT 3);
SELECT * FROM t_insert_select_parens ORDER BY x;
TRUNCATE TABLE t_insert_select_parens;

-- INSERT SELECT with EXCEPT and parentheses
INSERT INTO t_insert_select_parens (x) (SELECT * FROM numbers(5)) EXCEPT (SELECT * FROM numbers(3));
SELECT * FROM t_insert_select_parens ORDER BY x;
TRUNCATE TABLE t_insert_select_parens;

-- INSERT SELECT with INTERSECT and parentheses
INSERT INTO t_insert_select_parens (x) (SELECT * FROM numbers(5)) INTERSECT (SELECT * FROM numbers(3, 5));
SELECT * FROM t_insert_select_parens ORDER BY x;
TRUNCATE TABLE t_insert_select_parens;

-- Nested parentheses
INSERT INTO t_insert_select_parens (x) ((SELECT 10));
SELECT * FROM t_insert_select_parens ORDER BY x;
TRUNCATE TABLE t_insert_select_parens;

-- INSERT SELECT with CTE inside parentheses
INSERT INTO t_insert_select_parens (x) (WITH cte AS (SELECT 100 AS val) SELECT val FROM cte);
SELECT * FROM t_insert_select_parens ORDER BY x;

DROP TABLE t_insert_select_parens;
