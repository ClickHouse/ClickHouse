-- A matcher (`*`, `t.*`, `COLUMNS(...)`) as a LIMIT AFTER/UNTIL boundary is rejected, like in WHERE.

SELECT 1 AS x LIMIT 1 AFTER COLUMNS('zzz') UNTIL (x = 1); -- { serverError UNEXPECTED_EXPRESSION }
SELECT 1 AS x LIMIT 1 UNTIL COLUMNS('zzz'); -- { serverError UNEXPECTED_EXPRESSION }
SELECT number FROM numbers(3) LIMIT 1 UNTIL * EXCEPT number; -- { serverError UNEXPECTED_EXPRESSION }
SELECT number % 2 AS k, count() FROM numbers(6) GROUP BY k LIMIT 1 AFTER COLUMNS('zzz'); -- { serverError UNEXPECTED_EXPRESSION }
SELECT 'a', 29::Int64 GROUP BY ALL LIMIT 73 AFTER COLUMNS('5') UNTIL ('b' AS a0) FORMAT Null; -- { serverError UNEXPECTED_EXPRESSION }

-- Also when the matcher matches columns, instead of using the first one.
SELECT number FROM numbers(3) LIMIT 1 AFTER COLUMNS('number'); -- { serverError UNEXPECTED_EXPRESSION }
SELECT number FROM numbers(3) LIMIT 1 UNTIL *; -- { serverError UNEXPECTED_EXPRESSION }
SELECT number FROM numbers(3) AS t LIMIT 1 UNTIL t.*; -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b FROM (SELECT number AS a, number % 2 = 1 AS b FROM numbers(6)) LIMIT 2 AFTER COLUMNS('^[ab]$'); -- { serverError UNEXPECTED_EXPRESSION }

-- A boolean boundary still works.
SELECT number FROM numbers(6) LIMIT 2 AFTER number = 3 UNTIL number = 5;
SELECT number FROM numbers(6) LIMIT UNTIL number = 2;
