-- https://github.com/ClickHouse/ClickHouse/issues/36973
SET allow_experimental_analyzer=1;
SELECT 1, 1, 1;
SELECT * FROM (SELECT 1, 1, 1);
