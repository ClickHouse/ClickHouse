SELECT 'With Where:';

SELECT number FROM numbers(500) WHERE number >= 100 AND number <= 120 GROUP BY number;
SELECT number, AVG(4 * number - 7) FROM numbers(20) WHERE number >= 5 AND number <= 15 GROUP BY number;

SELECT 'Without Where:';

SELECT number % 25 + 30 AS k FROM numbers(1000) GROUP BY k;
SELECT 3 * (7 - number % 20) + 5 AS k, MAX(5 - number * 3) FROM numbers(300) GROUP BY k;

SELECT 'With Double Key:';

SELECT number % 30 + 5 AS a, 5 * (number % 20) - 4 AS b, MAX(number) FROM numbers(1000) GROUP BY a, b;
