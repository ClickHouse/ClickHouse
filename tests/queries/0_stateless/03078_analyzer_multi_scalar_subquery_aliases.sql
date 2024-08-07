-- https://github.com/ClickHouse/ClickHouse/issues/33825
SET enable_analyzer=1;
CREATE TABLE t2 (first_column Int64, second_column Int64) ENGINE = Memory;
INSERT INTO t2 SELECT number, number FROM system.numbers LIMIT 10;


SELECT (
        SELECT 111111111111
    ) AS first_column
FROM t2;

SELECT 1;

SELECT (
        SELECT 2222222222
    ) AS second_column
FROM t2;
