-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

SET enable_analyzer=1;
SET use_skip_indexes=1;

CREATE TABLE t_imv_minmax_basic (a UInt64)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    index_granularity = 1;

SYSTEM STOP MERGES t_imv_minmax_basic;
INSERT INTO t_imv_minmax_basic SELECT number FROM numbers(10);
INSERT INTO t_imv_minmax_basic SELECT number + 10 FROM numbers(10);
INSERT INTO t_imv_minmax_basic SELECT number + 20 FROM numbers(10);
INSERT INTO t_imv_minmax_basic SELECT number + 30 FROM numbers(10);

-- No level-0 part carries the index file (block_number / block_offset are provisional at insert).
explain indexes=1 SELECT * FROM t_imv_minmax_basic WHERE _block_offset > 2;

SYSTEM START MERGES t_imv_minmax_basic;
OPTIMIZE TABLE t_imv_minmax_basic FINAL;

-- Granule pruning works on the merged part.
select '';
explain indexes=1 SELECT * FROM t_imv_minmax_basic WHERE _block_number = 2;

select '';
explain indexes=1 SELECT * FROM t_imv_minmax_basic WHERE _block_offset > 2;
