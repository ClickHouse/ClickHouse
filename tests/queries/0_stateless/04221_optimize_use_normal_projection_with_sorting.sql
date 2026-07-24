-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET optimize_use_projections = 1;
SET optimize_read_in_order = 1;
SET optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS t_proj_sort;

CREATE TABLE t_proj_sort
(
    a UInt64,
    b UInt64,
    PROJECTION commit_order INDEX * TYPE commit_order
)
ENGINE = MergeTree
ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;

INSERT INTO t_proj_sort VALUES (3, 30) (1, 10) (2, 20);
INSERT INTO t_proj_sort VALUES (6, 60) (4, 40) (5, 50);
OPTIMIZE TABLE t_proj_sort FINAL;

SELECT '-- no WHERE, ORDER BY matches projection sorting key';
EXPLAIN SELECT * FROM t_proj_sort ORDER BY (_block_number, _block_offset);

SELECT '';
SELECT '-- WHERE present, ORDER BY matches projection sorting key';
EXPLAIN SELECT * FROM t_proj_sort WHERE a != 1 ORDER BY (_block_number, _block_offset);

SELECT '';
SELECT '-- no WHERE, no ORDER BY: projection is not helpful, fall back to main table';
EXPLAIN SELECT * FROM t_proj_sort;

SELECT '';
SELECT '-- read_in_order disabled: projection cannot help with sorting alone';
EXPLAIN SELECT * FROM t_proj_sort ORDER BY (_block_number, _block_offset) SETTINGS optimize_read_in_order = 0;

SELECT '';
SELECT '-- results respect commit order';
SELECT * FROM t_proj_sort ORDER BY (_block_number, _block_offset);
SELECT * FROM t_proj_sort WHERE a != 1 ORDER BY (_block_number, _block_offset);

DROP TABLE t_proj_sort;
