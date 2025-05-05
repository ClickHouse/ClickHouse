SELECT number, avg(DISTINCT number) OVER () FROM numbers(0, 5) ORDER BY number;
SELECT number, count(DISTINCT number) OVER () FROM numbers(0, 5) ORDER BY number;
SELECT number, max(DISTINCT number) OVER () FROM numbers(0, 5) ORDER BY number;
SELECT number, sum(DISTINCT number) OVER () FROM numbers(0, 5) ORDER BY number;
