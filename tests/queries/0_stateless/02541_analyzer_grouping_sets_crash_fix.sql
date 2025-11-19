SET enable_analyzer = 1;

WITH pow(NULL, 256) AS four SELECT NULL AS two GROUP BY GROUPING SETS ((pow(two, 65536)));

WITH (SELECT pow(two, 1) GROUP BY GROUPING SETS ((pow(1, 9)))) AS four SELECT 2 AS two GROUP BY pow(1, two);
