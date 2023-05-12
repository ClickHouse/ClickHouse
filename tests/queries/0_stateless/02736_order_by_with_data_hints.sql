SELECT number FROM numbers(500) WHERE number >= 100 AND number <= 120 ORDER BY number;
SELECT number, 8 - 4 * number FROM numbers(300) WHERE number >= 200 AND number <= 220 ORDER BY number;

SELECT number % 25 + 30 AS k, number FROM numbers(1000) WHERE number >= 500 AND number <= 550 ORDER BY k;
SELECT 3 * (7 - number % 10) + 5 AS k, number, 'Desc' FROM numbers(300) WHERE number >= 250 AND number <= 270 ORDER BY k DESC;

SELECT number % 3 + 5 AS a, 5 * (number % 20) - 4 AS b, number FROM numbers(1000) WHERE number >= 950 AND number <= 975 ORDER BY a, b;
