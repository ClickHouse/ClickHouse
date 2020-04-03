SELECT number FROM system.numbers LEFT ARRAY JOIN range(number % 3) AS arr LIMIT 10;
SELECT number, arr, x FROM (SELECT number, range(number % 3) AS arr FROM system.numbers LIMIT 10) LEFT ARRAY JOIN arr AS x;
