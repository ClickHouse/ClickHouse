-- https://github.com/ClickHouse/ClickHouse/issues/4432
SET enable_analyzer=1;
WITH [1, 2] AS zz
SELECT x
FROM system.one
ARRAY JOIN zz AS x
