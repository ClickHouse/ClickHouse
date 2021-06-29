SELECT 1 as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;

SELECT materialize(1) as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
SELECT materialize(1) as a, count() FROM numbers(10)　WHERE 0 ORDER BY a;

SELECT 2 as b, less(1, b) as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
SELECT upper('d') as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
