-- Conditions with subqueries should build sets in both analyzer and old-interpreter paths.
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number IN (SELECT 3);
SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number IN (SELECT 3);

SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number IN (SELECT 3) SETTINGS enable_analyzer = 0;
SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number IN (SELECT 3) SETTINGS enable_analyzer = 0;
