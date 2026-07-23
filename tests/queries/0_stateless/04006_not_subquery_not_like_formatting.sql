-- Verify that NOT (subquery) as operand of a comparison operator gets proper parentheses.
-- The issue was that NOT has lower precedence than comparison operators,
-- so NOT (subquery) NOT LIKE x was reparsed as NOT ((subquery) NOT LIKE x).
-- With the fix, the formatter produces (NOT (subquery)) NOT LIKE x which is stable.

SELECT formatQuery('SELECT (NOT (SELECT 1)) NOT LIKE \'x\'');
SELECT formatQuery('SELECT (NOT (SELECT 1)) LIKE \'x\'');
SELECT formatQuery('SELECT (NOT (SELECT 1)) NOT ILIKE \'x\'');
SELECT formatQuery('SELECT (NOT (SELECT 1)) ILIKE \'x\'');
SELECT formatQuery('SELECT (NOT (SELECT 1)) IN (1, 2, 3)');
SELECT formatQuery('SELECT (NOT (SELECT 1)) NOT IN (1, 2, 3)');
SELECT formatQuery('SELECT (NOT (SELECT 1)) = 1');
