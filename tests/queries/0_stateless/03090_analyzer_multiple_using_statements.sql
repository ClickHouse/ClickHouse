-- https://github.com/ClickHouse/ClickHouse/issues/55647
SET enable_analyzer=1;

SELECT
*
FROM (
    SELECT *
    FROM system.one
) a
JOIN (
    SELECT *
    FROM system.one
) b USING dummy
JOIN (
    SELECT *
    FROM system.one
) c USING dummy
