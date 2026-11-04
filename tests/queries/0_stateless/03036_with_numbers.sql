-- https://github.com/ClickHouse/ClickHouse/issues/13843
SET enable_analyzer=1;
WITH 10 AS n
SELECT *
FROM numbers(n);

WITH cast(10, 'UInt64') AS n
SELECT *
FROM numbers(n);
