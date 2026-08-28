-- DESCRIBE TABLE (SELECT ...) AS alias should work
-- https://github.com/ClickHouse/ClickHouse/issues/100031

DESCRIBE TABLE ( SELECT 1 AS a ) AS source FORMAT TSV;
DESCRIBE TABLE ( SELECT 1 AS a, 'hello' AS b ) AS source FORMAT TSV;

DESCRIBE TABLE ( SELECT 1 AS a ) FORMAT TSV;

DESCRIBE SELECT 1 AS a FORMAT TSV;

-- A parenthesized SELECT that is the first element of a set operation must still be parsed
-- as a query, not diverted to a table expression (which would only consume the first SELECT).
DESCRIBE (SELECT 1 AS a) UNION ALL (SELECT 2) FORMAT TSV;
DESCRIBE TABLE (SELECT 1 AS a) UNION ALL (SELECT 2) FORMAT TSV;

-- A single parenthesized subquery whose body is a set operation is still a table expression
-- and can carry a trailing alias.
DESCRIBE TABLE (SELECT 1 AS a UNION ALL SELECT 2) AS source FORMAT TSV;
DESCRIBE (SELECT 1 AS a UNION ALL SELECT 2) AS source FORMAT TSV;
DESCRIBE (SELECT 1 AS a UNION ALL SELECT 2) FORMAT TSV;
