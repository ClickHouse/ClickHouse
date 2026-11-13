-- Subcolumns as the version and is_deleted columns of ReplacingMergeTree.

DROP TABLE IF EXISTS t_vertical;
DROP TABLE IF EXISTS t_horizontal;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_other_subcolumns;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_materialized;
DROP TABLE IF EXISTS t_replicated;

SELECT '-- Wrong subcolumns';
CREATE TABLE t_error (key UInt64, meta Tuple(ver UInt64)) ENGINE = ReplacingMergeTree(meta.missing) ORDER BY key; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
CREATE TABLE t_error (key UInt64, meta Tuple(ver UInt64, del UInt16)) ENGINE = ReplacingMergeTree(meta.ver, meta.del) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE t_error (key UInt64, v Variant(UInt64, String)) ENGINE = ReplacingMergeTree(v.UInt64) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE t_error (key UInt64, j JSON) ENGINE = ReplacingMergeTree(j.ver) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }

SELECT '-- Tuple elements, vertical merge';
CREATE TABLE t_vertical (key UInt64, value String, meta Tuple(ver UInt32, del UInt8))
ENGINE = ReplacingMergeTree(meta.ver, meta.del) ORDER BY key
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- The row with the greater version is kept on insert, although it is not the last one.
INSERT INTO t_vertical SETTINGS optimize_on_insert = 1 VALUES (1, 'a', (2, 0)), (1, 'b', (1, 0)), (2, 'c', (1, 0)), (3, 'd', (1, 0));
SELECT * FROM t_vertical ORDER BY key;

INSERT INTO t_vertical SETTINGS optimize_on_insert = 0 VALUES (1, 'e', (3, 0)), (2, 'f', (0, 0)), (3, 'g', (2, 1));
-- A part with a key range that does not intersect with other parts.
INSERT INTO t_vertical SETTINGS optimize_on_insert = 0 VALUES (10, 'x', (1, 1)), (11, 'y', (1, 0));

SELECT * FROM t_vertical FINAL ORDER BY key SETTINGS enable_vertical_final = 0;
SELECT * FROM t_vertical FINAL ORDER BY key SETTINGS enable_vertical_final = 1;
SELECT key, value FROM t_vertical FINAL WHERE meta.ver > 1 ORDER BY key;
SELECT key, value FROM t_vertical FINAL PREWHERE meta.ver > 0 ORDER BY key;
-- Lazy FINAL is not applied to reading in order of the sorting key.
SELECT key, value FROM t_vertical FINAL WHERE value != 'b' ORDER BY key SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, optimize_read_in_order = 0;

OPTIMIZE TABLE t_vertical FINAL;
SELECT * FROM t_vertical ORDER BY key;

SELECT '-- ALTER';
ALTER TABLE t_vertical MODIFY COLUMN meta Tuple(ver UInt64, del UInt8);
INSERT INTO t_vertical SETTINGS optimize_on_insert = 0 VALUES (2, 'h', (5000000000, 0));
SELECT * FROM t_vertical FINAL ORDER BY key;

ALTER TABLE t_vertical DROP COLUMN meta; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical CLEAR COLUMN meta; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical RENAME COLUMN meta TO meta2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical MODIFY COLUMN meta Tuple(ver UInt64, del UInt8) ALIAS (toUInt64(1), toUInt8(0)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical MODIFY COLUMN meta Tuple(ver UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical MODIFY COLUMN meta Tuple(ver UInt32, del UInt8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical MODIFY COLUMN meta Tuple(ver Int64, del UInt8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical MODIFY COLUMN meta Tuple(ver UInt64, del UInt16); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical ADD COLUMN `meta.ver` UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_vertical UPDATE meta = (10, 0) WHERE 1; -- { serverError CANNOT_UPDATE_COLUMN }

DETACH TABLE t_vertical;
ATTACH TABLE t_vertical;
OPTIMIZE TABLE t_vertical FINAL;
SELECT * FROM t_vertical ORDER BY key;

SELECT '-- Tuple elements, horizontal merge with cleanup';
CREATE TABLE t_horizontal (key UInt64, value String, meta Tuple(ver UInt32, del UInt8))
ENGINE = ReplacingMergeTree(meta.ver, meta.del) ORDER BY key
SETTINGS enable_vertical_merge_algorithm = 0, allow_experimental_replacing_merge_with_cleanup = 1;

INSERT INTO t_horizontal SETTINGS optimize_on_insert = 0 VALUES (1, 'a', (2, 0)), (2, 'c', (1, 0)), (3, 'd', (1, 0));
INSERT INTO t_horizontal SETTINGS optimize_on_insert = 0 VALUES (1, 'e', (3, 0)), (2, 'f', (0, 0)), (3, 'g', (2, 1));
SELECT * FROM t_horizontal FINAL ORDER BY key;
OPTIMIZE TABLE t_horizontal FINAL CLEANUP;
SELECT * FROM t_horizontal ORDER BY key;

SELECT '-- JSON typed paths';
CREATE TABLE t_json (key UInt64, j JSON(ver UInt32, del UInt8))
ENGINE = ReplacingMergeTree(j.ver, j.del) ORDER BY key
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_json SETTINGS optimize_on_insert = 1 VALUES (1, '{"ver" : 2, "data" : "a"}'), (1, '{"ver" : 1, "data" : "b"}'), (2, '{"ver" : 1, "data" : "c"}');
INSERT INTO t_json SETTINGS optimize_on_insert = 0 VALUES (1, '{"ver" : 3, "data" : "e"}'), (2, '{"ver" : 2, "del" : 1, "data" : "f"}');
SELECT * FROM t_json FINAL ORDER BY key;
OPTIMIZE TABLE t_json FINAL;
SELECT * FROM t_json ORDER BY key;
-- Without the typed path the subcolumn becomes Dynamic, which cannot be a version.
ALTER TABLE t_json MODIFY COLUMN j JSON; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
-- A column with the name of the subcolumn would replace it.
ALTER TABLE t_json ADD COLUMN `j.ver` UInt32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_json ADD COLUMN `j.x` UInt32;
ALTER TABLE t_json RENAME COLUMN `j.x` TO `j.ver`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT '-- Other subcolumns';
-- The longest array wins, and a row with NULL is deleted.
CREATE TABLE t_other_subcolumns (key UInt64, arr Array(UInt8), n Nullable(String))
ENGINE = ReplacingMergeTree(arr.size0, n.null) ORDER BY key;

INSERT INTO t_other_subcolumns SETTINGS optimize_on_insert = 1 VALUES (1, [1, 2], 'a'), (1, [1], 'z'), (2, [1], 'b');
INSERT INTO t_other_subcolumns SETTINGS optimize_on_insert = 0 VALUES (1, [1, 2, 3], 'c'), (2, [1, 2], NULL);
INSERT INTO t_other_subcolumns SETTINGS optimize_on_insert = 0 VALUES (1, [1, 2], 'd');
SELECT * FROM t_other_subcolumns FINAL ORDER BY key;
OPTIMIZE TABLE t_other_subcolumns FINAL;
SELECT * FROM t_other_subcolumns ORDER BY key;
ALTER TABLE t_other_subcolumns ADD COLUMN `arr.size0` String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT '-- Nested';
CREATE TABLE t_nested (key UInt64, n Nested(arr UInt32, other String), val String, j JSON(a.b UInt8))
ENGINE = ReplacingMergeTree(n.arr.size0, j.a.b) ORDER BY key;

INSERT INTO t_nested SETTINGS optimize_on_insert = 0 VALUES (1, [1, 2, 3], ['a', 'b', 'c'], 'three, first', '{}');
INSERT INTO t_nested SETTINGS optimize_on_insert = 0 VALUES (1, [1], ['a'], 'one, second', '{}');
SELECT val FROM t_nested FINAL;

-- A command on the Nested group alters all of its columns.
ALTER TABLE t_nested CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_nested DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_nested DROP COLUMN `n.arr`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
-- The added Nested group has a column `j.a.b`, which would replace the subcolumn.
ALTER TABLE t_nested ADD COLUMN `j.a` Nested(b UInt8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_nested DROP COLUMN `n.other`;

DETACH TABLE t_nested;
ATTACH TABLE t_nested;
SELECT val FROM t_nested FINAL;

SELECT '-- MATERIALIZED column';
CREATE TABLE t_materialized (key UInt64, v UInt64, meta Tuple(ver UInt64) MATERIALIZED tuple(v))
ENGINE = ReplacingMergeTree(meta.ver) ORDER BY key
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_materialized (key, v) SETTINGS optimize_on_insert = 0 VALUES (1, 2), (1, 1);
-- Updating an input of the column would recompute the version.
ALTER TABLE t_materialized UPDATE v = 3 WHERE v = 1; -- { serverError CANNOT_UPDATE_COLUMN }
UPDATE t_materialized SET v = 3 WHERE v = 1; -- { serverError CANNOT_UPDATE_COLUMN }
SELECT v FROM t_materialized FINAL;

SELECT '-- Replicated';
CREATE TABLE t_replicated (key UInt64, value String, meta Tuple(ver UInt32, del UInt8))
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/t_replicated', 'r1', meta.ver, meta.del) ORDER BY key;

INSERT INTO t_replicated SETTINGS optimize_on_insert = 0 VALUES (1, 'a', (2, 0)), (2, 'b', (1, 0));
INSERT INTO t_replicated SETTINGS optimize_on_insert = 0 VALUES (1, 'c', (1, 0)), (2, 'd', (2, 1));
DETACH TABLE t_replicated;
ATTACH TABLE t_replicated;
SELECT * FROM t_replicated FINAL ORDER BY key;
SYSTEM SYNC REPLICA t_replicated;
OPTIMIZE TABLE t_replicated FINAL;
SELECT * FROM t_replicated ORDER BY key;

SELECT '-- Merge algorithms';
SYSTEM FLUSH LOGS part_log;
SELECT table, groupUniqArray(merge_algorithm) FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' AND table IN ('t_vertical', 't_horizontal', 't_json')
GROUP BY table ORDER BY table;

DROP TABLE t_vertical;
DROP TABLE t_horizontal;
DROP TABLE t_json;
DROP TABLE t_other_subcolumns;
DROP TABLE t_nested;
DROP TABLE t_materialized;
DROP TABLE t_replicated;
