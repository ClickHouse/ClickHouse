SELECT 'a' AS key, 'b' as value GROUP BY key WITH CUBE ORDER BY ALL SETTINGS allow_experimental_analyzer = 0;
SELECT 'a' AS key, 'b' as value GROUP BY key WITH CUBE ORDER BY ALL SETTINGS allow_experimental_analyzer = 1;

SELECT 'a' AS key, 'b' as value GROUP BY ignore(1) WITH CUBE ORDER BY ALL;

SELECT 'a' AS key, 'b' as value GROUP BY ignore(1);
SELECT 'a' AS key, 'b' as value GROUP BY key;
