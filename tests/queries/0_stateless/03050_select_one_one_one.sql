-- https://github.com/ClickHouse/ClickHouse/issues/36973
SET enable_analyzer=1;
SELECT 1, 1, 1;
SELECT * FROM (SELECT 1, 1, 1);
