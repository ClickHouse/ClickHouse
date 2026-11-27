-- A key that keeps both its cancel and its state row, with a merge block boundary between the two.
-- Every key lands on a boundary because a merge block holds two rows here.

DROP TABLE IF EXISTS t_collapsing_boundary_vertical;
DROP TABLE IF EXISTS t_collapsing_boundary_horizontal;
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_collapsing_boundary_vertical;
DROP TABLE IF EXISTS t_collapsing_boundary_horizontal;

CREATE TABLE t_collapsing_boundary_vertical (id UInt64, sign Int8, c1 UInt64, c2 UInt64)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS merge_max_block_size = 2, index_granularity = 8192, index_granularity_bytes = '10Mi',
         use_const_adaptive_granularity = 0, enable_vertical_merge_algorithm = 1,
         vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0,
         vertical_merge_algorithm_min_columns_to_activate = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_collapsing_boundary_horizontal (id UInt64, sign Int8, c1 UInt64, c2 UInt64)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS merge_max_block_size = 2, index_granularity = 8192, index_granularity_bytes = '10Mi',
         enable_vertical_merge_algorithm = 0,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_collapsing_boundary_vertical VALUES
    (1, -1, 101, 201),
    (2, -1, 102, 202), (2, 1, 122, 222),
    (3, -1, 103, 203), (3, 1, 123, 223),
    (4, 1, 104, 204),
    (5, 1, 105, 205), (5, -1, 115, 215);

INSERT INTO t_collapsing_boundary_horizontal VALUES
    (1, -1, 101, 201),
    (2, -1, 102, 202), (2, 1, 122, 222),
    (3, -1, 103, 203), (3, 1, 123, 223),
    (4, 1, 104, 204),
    (5, 1, 105, 205), (5, -1, 115, 215);

OPTIMIZE TABLE t_collapsing_boundary_vertical FINAL;
OPTIMIZE TABLE t_collapsing_boundary_horizontal FINAL;

SELECT 'vertical', id, sign, c1, c2 FROM t_collapsing_boundary_vertical ORDER BY id, c1;
SELECT 'horizontal', id, sign, c1, c2 FROM t_collapsing_boundary_horizontal ORDER BY id, c1;

-- A merged block is a granule for this table, so the mark count shows the merge wrote several blocks.
SELECT 'vertical marks', marks FROM system.parts
WHERE database = currentDatabase() AND table = 't_collapsing_boundary_vertical' AND active ORDER BY name;

DROP TABLE t_collapsing_boundary_vertical;
DROP TABLE t_collapsing_boundary_horizontal;
