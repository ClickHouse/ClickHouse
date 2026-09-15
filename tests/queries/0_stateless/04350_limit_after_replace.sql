-- SELECT * REPLACE(expr AS col) rewrites downstream references to `col` into `expr`. The AFTER/UNTIL
-- range predicates must be rewritten too, otherwise they bind to the original column and open/close
-- the range on the wrong boundary.

-- { echo }

-- a is 0..9, REPLACE(a*10 AS a). AFTER a > 50 must use the replaced value (a*10 > 50 => a >= 6),
-- so the range opens at 60. Without the rewrite it would bind to the raw a (0..9 > 50 => empty).
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10)) ORDER BY a LIMIT 3 AFTER a > 50;
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10)) ORDER BY a LIMIT 3 AFTER a > 50 SETTINGS enable_analyzer = 0;

-- UNTIL with REPLACE: a*10 until a*10 >= 30 => stop before 30, open from start.
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10)) ORDER BY a LIMIT UNTIL a >= 30;
SELECT * REPLACE(a * 10 AS a) FROM (SELECT number AS a FROM numbers(10)) ORDER BY a LIMIT UNTIL a >= 30 SETTINGS enable_analyzer = 0;
