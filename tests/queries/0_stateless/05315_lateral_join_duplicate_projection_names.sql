-- A `LATERAL` subquery can project two different columns with the same name: here an inner `number`
-- and the correlated outer `o.number`. Like for any other subquery in `FROM`, the first column
-- of that name is the one bound to `s.number` (and to both columns of `s.*`).

SET allow_experimental_lateral_join = 1;

SELECT 'inner first';
SELECT o.number, s.*
FROM numbers(2) AS o
LEFT JOIN LATERAL (SELECT number, o.number FROM numbers(10, 1)) AS s ON true
ORDER BY ALL;

SELECT 'outer first';
SELECT o.number, s.*
FROM numbers(2) AS o
LEFT JOIN LATERAL (SELECT o.number, number FROM numbers(10, 1)) AS s ON true
ORDER BY ALL;

SELECT 'qualified';
SELECT o.number, s.number
FROM numbers(2) AS o
INNER JOIN LATERAL (SELECT number, o.number FROM numbers(10, 1)) AS s ON true
ORDER BY ALL;

SELECT 'same expression twice';
SELECT o.number, s.*
FROM numbers(2) AS o
LEFT JOIN LATERAL (SELECT o.number * 10, o.number * 10) AS s ON true
ORDER BY ALL;
