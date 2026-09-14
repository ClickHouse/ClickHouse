-- Lightweight updates on a table whose sorting key uses subcolumns. v2 patch parts
-- store the sort-key source columns; a subcolumn (e.g. `tup.a`) is read as a subcolumn
-- from the main table and stored in the patch part as a physical column.

DROP TABLE IF EXISTS t_lwu_subcolumn_key;

CREATE TABLE t_lwu_subcolumn_key (tup Tuple(a UInt64, b UInt64), v UInt64)
ENGINE = MergeTree ORDER BY (tup.a, tup.b)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         patch_parts_version = 'v2', apply_patches_on_merge = 1;

INSERT INTO t_lwu_subcolumn_key SELECT (number % 10, number), number FROM numbers(1000);

UPDATE t_lwu_subcolumn_key SET v = v + 1000000 WHERE tup.a = 3;

SELECT count() FROM t_lwu_subcolumn_key WHERE v >= 1000000;
SELECT count() FROM t_lwu_subcolumn_key WHERE v < 1000000 AND tup.a != 3;

-- Patch application on merge materializes the update.
OPTIMIZE TABLE t_lwu_subcolumn_key FINAL;
SELECT count() FROM t_lwu_subcolumn_key WHERE v >= 1000000 SETTINGS apply_patch_parts = 0;

-- A new patch on the merged part still applies on read.
UPDATE t_lwu_subcolumn_key SET v = v + 1000000 WHERE tup.a = 4;
SELECT count() FROM t_lwu_subcolumn_key WHERE v >= 1000000;

DROP TABLE t_lwu_subcolumn_key;
