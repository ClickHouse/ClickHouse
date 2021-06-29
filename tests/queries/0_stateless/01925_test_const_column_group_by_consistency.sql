SELECT 1 as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
SELECT materialize(1) as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
