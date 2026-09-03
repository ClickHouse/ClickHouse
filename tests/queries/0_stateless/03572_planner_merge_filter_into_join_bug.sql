SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

SELECT table1.id1 AS id1,
       table1.date1 AS date1
FROM
    (SELECT 1 AS id1, toDateTime('2025-01-01 01:00:00') AS date1) AS table1
    JOIN
    (SELECT 1 AS id2, toDate('2025-01-01') AS date2) AS p
    ON table1.id1 = p.id2
WHERE toDate(table1.date1) = p.date2;
