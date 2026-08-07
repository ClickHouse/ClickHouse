-- https://github.com/ClickHouse/ClickHouse/issues/8030

SET enable_analyzer=1;

DESCRIBE (SELECT 1, 1 UNION ALL SELECT 1, 2);
