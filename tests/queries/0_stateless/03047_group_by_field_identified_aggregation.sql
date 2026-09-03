-- https://github.com/ClickHouse/ClickHouse/issues/32639
SET enable_analyzer=1;

SELECT 0 AND id ? 1 : 2 AS a, sum(id) FROM (SELECT 1 AS id) GROUP BY a;
