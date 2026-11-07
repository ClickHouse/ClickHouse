-- Regression test for `group_by_each_block_no_merge` together with `WITH TOTALS`, `max_rows_to_group_by` and
-- `group_by_overflow_mode = 'any'` (`totals_mode = 'before_having'` enables the overflow row). With the
-- streaming setting every per-block flush emits its own `is_overflows` chunk, while `TotalsHavingTransform`
-- used to keep only the last one. Note that rows cannot actually reach the overflow row in the streaming mode:
-- the `max_rows_to_group_by` latch (`no_more_keys`) is only set after a whole block was consumed and is reset
-- together with the per-block state, so the per-block overflow chunks carry empty aggregation states. Still,
-- `TotalsHavingTransform` now accumulates all of them, and this test pins the end-to-end contract: the totals
-- row must aggregate all input rows.
--
-- The inner query reads in a single thread, so the blocks are exactly [0, 10), [10, 20), [20, 30) in order;
-- two-level aggregation is disabled so the block contents are deterministic. Each block has 10 distinct keys
-- and therefore crosses `max_rows_to_group_by`, producing an overflow chunk per block.
SELECT number AS k, count() AS c FROM numbers(30) GROUP BY k WITH TOTALS ORDER BY k
SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1,
         group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0,
         max_rows_to_group_by = 5, group_by_overflow_mode = 'any', totals_mode = 'before_having';
