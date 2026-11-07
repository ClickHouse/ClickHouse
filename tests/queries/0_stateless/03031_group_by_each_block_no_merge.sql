SET group_by_each_block_no_merge = 1;
SET max_block_size = 1000;

-- `group_by_each_block_no_merge` aggregates and flushes the result for every block separately, without merging.
-- The produced rows therefore depend on how the input is split into blocks. To keep the result deterministic we
-- read `system.numbers` in a single thread, so the blocks are exactly [0, 1000), [1000, 2000), ... in order.
SET max_threads = 1;

-- Disable two-level aggregation: it would emit the per-block rows in bucket (hash) order, which changes
-- which rows survive the inner `LIMIT 100` and hence the result.
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;

SELECT * FROM (SELECT number DIV 113 AS k, count() AS c, sum(number) FROM system.numbers GROUP BY ALL LIMIT 100) ORDER BY k;
