-- Test for https://github.com/ClickHouse/ClickHouse/issues/95744
-- Fix: Not-ready Set should not throw exception in dry-run mode (EXPLAIN PIPELINE)

-- This query would previously fail with:
-- Logical error: 'Not-ready Set is passed as the second argument for function 'globalNotIn''.
-- because during query plan optimization, the IN function was called in dry-run mode
-- but the Set was not ready yet.

set ignore_format_null_for_explain = 0;

EXPLAIN PIPELINE
WITH cte AS (SELECT number FROM numbers(10))
SELECT * FROM cte WHERE number GLOBAL NOT IN (SELECT number FROM cte)
FORMAT Null;

SELECT 'OK';
