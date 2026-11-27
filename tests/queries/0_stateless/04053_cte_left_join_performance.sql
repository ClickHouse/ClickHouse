-- Tags: long, no-flaky-check, no-sanitizers
-- https://github.com/ClickHouse/ClickHouse/issues/47713
-- Verify that CTE with LEFT JOIN runs quickly and does not hang.

-- joined_block_split_single_row disables lazy join counting, turning O(n) probe into O(n²).
SET joined_block_split_single_row = 0;

WITH t AS (SELECT 0 AS key, number AS x FROM numbers_mt(1000000))
SELECT count() FROM t AS a LEFT JOIN t AS b ON a.key = b.key;
