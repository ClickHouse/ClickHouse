-- { echo }


SELECT number AS a, number + 1 AS b, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 1 BY g;


SELECT number AS a, number + 1 AS b, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -1 BY g;
