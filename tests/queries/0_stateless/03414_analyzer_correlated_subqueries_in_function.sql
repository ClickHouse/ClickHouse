set enable_analyzer = 1;
set allow_experimental_correlated_subqueries = 1;

SELECT count()
FROM numbers(3) AS t
WHERE 1 IN (
    SELECT 1
    FROM numbers(3)
    WHERE number = t.number
); -- { serverError NOT_IMPLEMENTED }

SELECT count()
FROM numbers(3) AS t
WHERE (SELECT count() FROM numbers(3) WHERE number = t.number) IN (1); -- { serverError NOT_IMPLEMENTED }
