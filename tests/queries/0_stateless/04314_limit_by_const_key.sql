SELECT 'all-const, regular' AS scenario, count() AS rows_kept FROM (SELECT number FROM numbers(10) LIMIT 3 BY ignore(number));
SELECT 'all-const, negative', count() FROM (SELECT number FROM numbers(10) LIMIT -3 BY ignore(number));
SELECT 'all-const, old planner', count() FROM (SELECT number FROM numbers(10) LIMIT 3 BY ignore(number)) SETTINGS enable_analyzer = 0;

SELECT 'const + real key, generic', count() FROM (SELECT number, number % 4 AS g FROM numbers(20) LIMIT 1 BY g, ignore(number));
SELECT 'const + real key, sorted', count() FROM (SELECT number, number % 4 AS g FROM numbers(20) ORDER BY g, number LIMIT 1 BY g, ignore(number));
SELECT 'const + real key, negative', count() FROM (SELECT number, number % 4 AS g FROM numbers(20) ORDER BY g, number LIMIT -1 BY g, ignore(number));
