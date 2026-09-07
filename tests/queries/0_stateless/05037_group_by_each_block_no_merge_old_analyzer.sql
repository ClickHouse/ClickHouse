-- `group_by_each_block_no_merge` is wired into the aggregation parameters separately for the analyzer
-- (`Planner`) and for the old query interpreter (`InterpreterSelectQuery`). Cover the old interpreter too:
-- the setting must be off by default there (a plain, fully merged `GROUP BY`) and take effect when enabled.

SET enable_analyzer = 0;
SET max_block_size = 1000;
SET max_threads = 1;
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;

-- Off by default: two rows, fully merged across all the blocks.
SELECT k, c FROM (SELECT number % 2 AS k, count() AS c FROM numbers(10000) GROUP BY k) ORDER BY k, c;

-- Enabled: every block is aggregated and flushed on its own, so each of the 10 blocks contributes its own pair of rows.
SET group_by_each_block_no_merge = 1;
SELECT k, c FROM (SELECT number % 2 AS k, count() AS c FROM numbers(10000) GROUP BY k) ORDER BY k, c;
