SELECT number FROM numbers(3) GROUP BY (number, number % 2) ORDER BY number;
