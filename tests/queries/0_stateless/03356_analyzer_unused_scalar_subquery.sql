set enable_analyzer = 1;

WITH (
        SELECT sleepEachRow(2)
    ) AS res
SELECT *
FROM system.one
FORMAT Null
SETTINGS max_execution_time = 1;

WITH sleepEachRow(2) AS res
SELECT *
FROM system.one
FORMAT Null
SETTINGS max_execution_time = 1;

