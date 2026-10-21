-- https://github.com/ClickHouse/ClickHouse/issues/116622
-- A patch part produced by a lightweight `UPDATE` holds the updated values of rows that already live
-- in the base parts, so counting it into the table's active-rows total reported more rows than the
-- table has - and the trivial-count path served that number as `count()`.

DROP TABLE IF EXISTS t_patch_total_rows;
CREATE TABLE t_patch_total_rows (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_patch_total_rows SELECT number, number FROM numbers(1000);
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 't_patch_total_rows';

UPDATE t_patch_total_rows SET v = v + 1 WHERE id < 100;

SELECT count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_patch_total_rows' AND active AND name LIKE 'patch%';
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 't_patch_total_rows';
SELECT count() FROM t_patch_total_rows;
SELECT count() FROM t_patch_total_rows SETTINGS apply_patch_parts = 0;
SELECT count() FROM t_patch_total_rows SETTINGS apply_patch_parts = 0, optimize_trivial_count_query = 0;
SELECT count() FROM t_patch_total_rows SETTINGS optimize_trivial_count_query = 0;
SELECT sum(v) FROM t_patch_total_rows;

SELECT 'the byte counters exclude the patch as well';
SELECT total_bytes = (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = currentDatabase() AND table = 't_patch_total_rows' AND active AND part_type != 'Unknown' AND name NOT LIKE 'patch%')
FROM system.tables WHERE database = currentDatabase() AND name = 't_patch_total_rows';

DROP TABLE t_patch_total_rows;
