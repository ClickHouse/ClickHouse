-- https://github.com/ClickHouse/ClickHouse/issues/36973
SELECT 1, 1, 1;
SELECT * FROM (SELECT 1, 1, 1);
