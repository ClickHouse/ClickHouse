-- Tags: no-parallel
-- https://github.com/ClickHouse/ClickHouse/issues/92982
-- NOT_FOUND_COLUMN_IN_BLOCK exception for valid query with constant Columns and DISTINCT, ORDER BY, LIMIT BY

SELECT DISTINCT 1, '1' ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=1;
SELECT DISTINCT 1, '1' ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=0;

-- Test with different constant types
SELECT DISTINCT 1, '1', 2.5 ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=1;

-- Test without DISTINCT
SELECT 1, '1' ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=1;

-- Test without ORDER BY
SELECT DISTINCT 1, '1' LIMIT 1 BY 2 SETTINGS enable_analyzer=1;

-- Test with more complex constants
SELECT DISTINCT number % 2, 'const' FROM numbers(5) ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=1;

