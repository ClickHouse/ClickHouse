-- https://github.com/ClickHouse/ClickHouse/issues/61950
SET enable_analyzer=1;

with dummy + 1 as dummy select dummy from system.one;

WITH dummy + 3 AS dummy
SELECT dummy + 1 AS y
FROM system.one
SETTINGS enable_global_with_statement = 1;
