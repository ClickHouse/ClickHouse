-- Tags: long, no-flaky-check, no-sanitizers
-- https://github.com/ClickHouse/ClickHouse/issues/47713
-- Verify that CTE with LEFT JOIN runs quickly and does not hang.

-- joined_block_split_single_row disables lazy join counting, turning O(n) probe into O(n²).
SET joined_block_split_single_row = 0;
-- Override max_threads to prevent the CI randomizer from setting it to 1,
-- which combined with joined_block_split_single_row can push the test over the 10-minute limit.
SET max_threads = 4;

WITH t AS (SELECT 0 AS key, number AS x FROM numbers_mt(1000000))
SELECT count() FROM t AS a LEFT JOIN t AS b ON a.key = b.key;
