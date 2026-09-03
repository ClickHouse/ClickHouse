-- Regression test for `group_by_each_block_no_merge` together with `max_rows_to_group_by` and
-- `group_by_overflow_mode = 'any'`. Each block is aggregated independently, so the limit must apply per
-- block too. Previously `no_more_keys` was latched to `true` by the first block that crossed the limit and
-- never reset, so the fresh state of the following blocks rejected all new keys and emitted no real group
-- rows. After the fix `no_more_keys` is reset together with the per-block state, so every block produces its
-- own partial results.

-- The streaming setting and the limits are applied only to the inner aggregation, so the outer aggregation
-- (which checks the result) runs normally. The inner query reads in a single thread, so the blocks are
-- exactly [0, 1000), [1000, 2000), [2000, 3000) in order; two-level aggregation is disabled so the block
-- contents are deterministic. Each block has 1000 distinct keys and therefore crosses `max_rows_to_group_by`,
-- latching `no_more_keys`. Every block must still emit real group rows of its own; with the bug only the
-- first block (keys < 1000) does.
SELECT
    countIf(k < 1000) > 0,
    countIf(k >= 1000 AND k < 2000) > 0,
    countIf(k >= 2000) > 0
FROM
(
    SELECT number AS k FROM numbers(3000) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000, max_threads = 1,
             group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0,
             max_rows_to_group_by = 100, group_by_overflow_mode = 'any'
);
