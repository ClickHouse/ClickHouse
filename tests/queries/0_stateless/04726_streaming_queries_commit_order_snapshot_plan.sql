-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree, no-parallel-replicas

SET enable_analyzer = 1;
SET optimize_use_projections = 1;
SET optimize_read_in_order = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_commit_order_snapshot SYNC;

CREATE TABLE t_commit_order_snapshot
(
    a UInt64,
    PROJECTION commit_order INDEX * TYPE commit_order
)
ENGINE = MergeTree
ORDER BY a
SETTINGS enable_block_number_column = 1,
         enable_block_offset_column = 1,
         allow_commit_order_projection = 1,
         part_minmax_index_columns = 'with_block_number_offset',
         add_minmax_index_for_block_number_column = 1,
         add_minmax_index_for_block_offset_column = 1,
         index_granularity = 1,
         merge_selector_algorithm = 'Manual';

INSERT INTO t_commit_order_snapshot SELECT number FROM numbers(16); -- all_1_1_0
INSERT INTO t_commit_order_snapshot SELECT number FROM numbers(16); -- all_2_2_0
INSERT INTO t_commit_order_snapshot SELECT number FROM numbers(16); -- all_3_3_0
INSERT INTO t_commit_order_snapshot SELECT number FROM numbers(16); -- all_4_4_0
INSERT INTO t_commit_order_snapshot SELECT number FROM numbers(16); -- all_5_5_0
INSERT INTO t_commit_order_snapshot SELECT number FROM numbers(16); -- all_6_6_0
INSERT INTO t_commit_order_snapshot SELECT number FROM numbers(16); -- all_7_7_0
INSERT INTO t_commit_order_snapshot SELECT number FROM numbers(16); -- all_8_8_0

SYSTEM SCHEDULE MERGE t_commit_order_snapshot PARTS 'all_2_2_0', 'all_3_3_0'; -- all_2_3_1
SYSTEM SCHEDULE MERGE t_commit_order_snapshot PARTS 'all_4_4_0', 'all_5_5_0'; -- all_4_5_1
SYSTEM SCHEDULE MERGE t_commit_order_snapshot PARTS 'all_6_6_0', 'all_7_7_0'; -- all_6_7_1
SYSTEM SYNC MERGES t_commit_order_snapshot;

SELECT '-- cursor over blocks [4, 6]: only merged parts, direct in-order commit_order projection read, consumed and future parts pruned by part minmax';
EXPLAIN indexes = 1, projections = 1
SELECT a, _partition_id, _block_number, _block_offset
FROM t_commit_order_snapshot
WHERE _partition_id = 'all' AND _block_number <= 6 AND (_block_number > 3 OR (_block_number = 3 AND _block_offset > 15))
ORDER BY _block_number, _block_offset;

SELECT '';
SELECT '-- cursor over blocks [4, 8]: merged parts read via the projection, level-0 parts via the main table';
EXPLAIN indexes = 1, projections = 1
SELECT a, _partition_id, _block_number, _block_offset
FROM t_commit_order_snapshot
WHERE _partition_id = 'all' AND _block_number <= 8 AND (_block_number > 3 OR (_block_number = 3 AND _block_offset > 15))
ORDER BY _block_number, _block_offset;
