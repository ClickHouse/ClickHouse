-- Conditions with subqueries should build their sets.
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number IN (SELECT 3);
SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number IN (SELECT 3);
