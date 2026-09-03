-- https://github.com/ClickHouse/ClickHouse/issues/40955
SET enable_analyzer=1;
WITH toInt64(2) AS new_x SELECT new_x AS x FROM (SELECT 1 AS x) t;
WITH toInt64(2) AS new_x SELECT * replace(new_x as x)  FROM (SELECT 1 AS x) t;
SELECT 2 AS x FROM (SELECT 1 AS x) t;
SELECT * replace(2 as x)  FROM (SELECT 1 AS x) t;
