SELECT 1 as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;

SELECT materialize(1) as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
SELECT materialize(1) as a, count() FROM numbers(10)　WHERE 0 ORDER BY a;

SELECT isConstant(1) as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
SELECT 1 as b, isConstant(b) as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
SELECT 0 as b, least(isConstant(materialize(1)), b) as a, count() FROM numbers(10)　WHERE 0 GROUP BY a;
