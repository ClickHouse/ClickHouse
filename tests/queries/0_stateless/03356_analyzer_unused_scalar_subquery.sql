set enable_analyzer = 1;

WITH (
        SELECT sleepEachRow(3)
    ) AS res
SELECT *
FROM system.one
FORMAT Null
SETTINGS max_execution_time = 2;

WITH sleepEachRow(3) AS res
SELECT *
FROM system.one
FORMAT Null
SETTINGS max_execution_time = 2;

