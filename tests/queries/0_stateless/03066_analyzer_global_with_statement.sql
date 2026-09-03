SET enable_analyzer=1;
WITH 0 AS test
SELECT *
FROM
(
    SELECT 1 AS test
)
SETTINGS enable_global_with_statement = 1
