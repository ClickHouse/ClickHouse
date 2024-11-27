-- This query succeeds only if it is correctly optimized.
SET enable_analyzer = 1;
SELECT * FROM (SELECT * FROM numbers(1e19)) AS t1, (SELECT * FROM numbers(1e19)) AS t2 WHERE t1.number IN (123, 456) AND t2.number = t1.number ORDER BY ALL;

-- Still TODO:
-- SELECT * FROM (SELECT * FROM numbers(1e19)) AS t1, (SELECT * FROM numbers(1e19)) AS t2 WHERE t1.number IN (SELECT 123 UNION ALL SELECT 456) AND t2.number = t1.number ORDER BY ALL;
