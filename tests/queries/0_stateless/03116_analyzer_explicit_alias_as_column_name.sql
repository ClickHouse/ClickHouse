-- https://github.com/ClickHouse/ClickHouse/issues/39923
SET allow_experimental_analyzer=1;

SELECT
    errors.name AS labels,
    value,
    'ch_errors_total' AS name
FROM system.errors
LIMIT 1
FORMAT Null;


SELECT
    map('name', errors.name) AS labels,
    value,
    'ch_errors_total' AS name
FROM system.errors
LIMIT 1
FORMAT Null;
