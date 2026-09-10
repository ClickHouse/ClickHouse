-- Boundary expressions accept an alias like the expressions of every other clause, so formatting a query
-- whose boundary carries one is stable.
SELECT formatQuery('SELECT 1 LIMIT AFTER (1 AS x)');
SELECT formatQuery(formatQuery('SELECT 1 LIMIT AFTER (1 AS x)'));
SELECT formatQuery(formatQuery('SELECT 1 LIMIT UNTIL (0 AS x)'));
SELECT 1 LIMIT AFTER (1 AS x);
SELECT number FROM numbers(3) LIMIT AFTER number >= 1 AS start;
