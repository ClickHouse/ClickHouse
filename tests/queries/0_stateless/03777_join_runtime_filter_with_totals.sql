SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET enable_join_runtime_filters=1;

SELECT n
FROM (SELECT number%2 AS n FROM numbers(6) GROUP BY n WITH TOTALS) AS left
INNER JOIN (SELECT number%3 AS n FROM numbers(6) GROUP BY n WITH TOTALS) AS right
USING (n)
ORDER BY n;
