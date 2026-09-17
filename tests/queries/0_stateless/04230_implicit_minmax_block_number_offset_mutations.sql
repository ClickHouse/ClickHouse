-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

SET explain_query_plan_default = 'legacy';
SET enable_analyzer=1;
SET use_skip_indexes=1;

CREATE TABLE t_imv_minmax_mut (a UInt64, b UInt64)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    index_granularity = 4,
    mutations_sync = 1;

INSERT INTO t_imv_minmax_mut SELECT number, number FROM numbers(10);
INSERT INTO t_imv_minmax_mut SELECT number + 10, number FROM numbers(10);
INSERT INTO t_imv_minmax_mut SELECT number + 20, number FROM numbers(10);
INSERT INTO t_imv_minmax_mut SELECT number + 30, number FROM numbers(10);
OPTIMIZE TABLE t_imv_minmax_mut FINAL;

explain indexes=1 SELECT * FROM t_imv_minmax_mut WHERE _block_number = 2;

ALTER TABLE t_imv_minmax_mut UPDATE b = b + 1 WHERE a >= 8 SETTINGS mutations_sync=2;

select '';
explain indexes=1 SELECT * FROM t_imv_minmax_mut WHERE _block_number = 2;

ALTER TABLE t_imv_minmax_mut DELETE WHERE a < 4 SETTINGS mutations_sync=2;

select '';
explain indexes=1 SELECT * FROM t_imv_minmax_mut WHERE _block_number = 2;
