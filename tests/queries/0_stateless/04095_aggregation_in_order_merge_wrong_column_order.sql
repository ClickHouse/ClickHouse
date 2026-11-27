-- Tags: no-random-merge-tree-settings

-- Regression test for incorrect column comparison in FinishAggregatingInOrderAlgorithm.
--
-- When GROUP BY column order differs from the table's sorting key order
-- (e.g., GROUP BY a, b on a table ORDER BY (b, a)), the sort description
-- passed to FinishAggregatingInOrderAlgorithm has columns in sort-key
-- order [b, a] with column_numbers [1, 0] (header positions), while
-- sorting_columns was built in iteration order [0→b, 1→a] via emplace_back.
--
-- The less() function indexes sorting_columns by column_number (header
-- position), so it accessed sorting_columns[1]=a when it meant column b,
-- and sorting_columns[0]=b when it meant column a — swapped comparisons.
-- This corrupted merge group boundaries, causing duplicate rows.

SET optimize_aggregation_in_order = 1;

DROP TABLE IF EXISTS t_inorder_merge_order;

-- Table sorted by (b, a). Both GROUP BY keys are in the sort key, so the
-- optimizer will use a 2-column sort description [b, a] for in-order
-- aggregation. The header has [a (pos 0), b (pos 1), aggregate (pos 2)],
-- creating the column_number mismatch that triggers the bug.
CREATE TABLE t_inorder_merge_order (a String, b UInt32, c UInt32)
ENGINE = MergeTree ORDER BY (b, a)
SETTINGS index_granularity = 128;

-- Stop merges to guarantee multiple parts survive until the SELECT.
-- Without this, a fast release binary can merge all parts into one before
-- the query runs, leaving only one stream and never triggering the
-- multi-stream merge bug in FinishAggregatingInOrderAlgorithm.
SYSTEM STOP MERGES t_inorder_merge_order;

-- Create multiple parts to ensure parallel in-order aggregation streams
INSERT INTO t_inorder_merge_order SELECT 'key_' || toString(rand() % 100), number % 200, number FROM numbers(500000);
INSERT INTO t_inorder_merge_order SELECT 'key_' || toString(rand() % 100), number % 200, number FROM numbers(500000);
INSERT INTO t_inorder_merge_order SELECT 'key_' || toString(rand() % 100), number % 200, number FROM numbers(500000);
INSERT INTO t_inorder_merge_order SELECT 'key_' || toString(rand() % 100), number % 200, number FROM numbers(500000);

-- The query must produce exactly 20000 distinct (a, b) groups.
-- Before the fix, with multiple streams and small block sizes, swapped column
-- comparison in the merge step produced duplicate rows (up to 37000+).
SELECT count()
FROM (
    SELECT a, b, count() as cnt
    FROM t_inorder_merge_order
    GROUP BY a, b
)
SETTINGS max_threads = 4, max_block_size = 128, aggregation_in_order_max_block_bytes = 1000;

-- Also test with the duplicate GROUP BY key variant (original failing test pattern)
SELECT count()
FROM (
    SELECT a, b, count() as cnt
    FROM t_inorder_merge_order
    GROUP BY a, b, b
)
SETTINGS max_threads = 4, max_block_size = 128, aggregation_in_order_max_block_bytes = 1000;

DROP TABLE t_inorder_merge_order;
