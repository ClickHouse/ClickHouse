-- Tags: no-random-merge-tree-settings

-- A per-block function such as `blockSize` is not stateful, but its result still depends on how the rows reaching
-- it are batched. Skipping leading granules changes the block boundaries the offset step passes on, so the
-- OFFSET-skip read-in-order optimization must bail out on any function that is not deterministic within the
-- query, not only on stateful ones. Every query below must return the same values with the optimization enabled
-- and disabled, and the enabled run must read as many rows as the disabled one.

DROP TABLE IF EXISTS t_skip_offset_per_block;
CREATE TABLE t_skip_offset_per_block (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_skip_offset_per_block SELECT number FROM numbers(10);

SET optimize_read_in_order = 1, max_threads = 1, max_block_size = 1, log_queries = 1;

SELECT 'blockSize under LIMIT/OFFSET, disabled';
SELECT k, blockSize() AS s FROM t_skip_offset_per_block ORDER BY k LIMIT 2 OFFSET 3 /* marker: block_size_disabled */
SETTINGS query_plan_optimize_read_in_order_skip_offset = 0;
SELECT 'blockSize under LIMIT/OFFSET, enabled';
SELECT k, blockSize() AS s FROM t_skip_offset_per_block ORDER BY k LIMIT 2 OFFSET 3 /* marker: block_size_enabled */
SETTINGS query_plan_optimize_read_in_order_skip_offset = 1;

SELECT 'blockSize under a pure OFFSET, disabled';
SELECT k, s FROM (SELECT k, blockSize() AS s FROM t_skip_offset_per_block ORDER BY k) OFFSET 3
SETTINGS query_plan_optimize_read_in_order_skip_offset = 0;
SELECT 'blockSize under a pure OFFSET, enabled';
SELECT k, s FROM (SELECT k, blockSize() AS s FROM t_skip_offset_per_block ORDER BY k) OFFSET 3
SETTINGS query_plan_optimize_read_in_order_skip_offset = 1;

SELECT 'plain projection, enabled';
SELECT k FROM t_skip_offset_per_block ORDER BY k LIMIT 2 OFFSET 3 /* marker: plain_enabled */
SETTINGS query_plan_optimize_read_in_order_skip_offset = 1;

SYSTEM FLUSH LOGS query_log;

SELECT 'read_rows per query';
SELECT extract(query, 'marker: (\\w+)') AS marker, read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%marker: %' AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_skip_offset_per_block;
