-- { echo }

SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 2 BY g SETTINGS enable_analyzer = 0;

SELECT number AS a, number + 1 AS b, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 1 BY g SETTINGS enable_analyzer = 1;

SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 BY g SETTINGS enable_analyzer = 0;

SELECT number AS a, number + 1 AS b, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -1 BY g SETTINGS enable_analyzer = 1;
