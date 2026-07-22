-- Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage
-- why: core column-ID coverage (writes, merges, mutations, introspection); no-object-storage because the introspection section asserts on ID-based stream filenames of local parts.

SET allow_experimental_column_ids = 1;

-- why: a column added after CREATE gets a counter ID; both parts stay readable.
CREATE TABLE t_ids_basic (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_basic VALUES (1, 'one');
ALTER TABLE t_ids_basic ADD COLUMN c Nullable(String);
INSERT INTO t_ids_basic (a, b, c) VALUES (2, 'two', 'second');
SELECT * FROM t_ids_basic ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_basic' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_basic SYNC;

-- why: row-level virtuals (_block_number, _block_offset, _row_exists) must coexist with column IDs.
CREATE TABLE t_ids_virtuals (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids',
    enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_ids_virtuals VALUES (1);
ALTER TABLE t_ids_virtuals ADD COLUMN c UInt64 DEFAULT a + 10;
INSERT INTO t_ids_virtuals (a, c) VALUES (2, 22);
SELECT a, c FROM t_ids_virtuals ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_virtuals' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT countDistinct(_block_number), sum(_block_offset) FROM t_ids_virtuals;
DELETE FROM t_ids_virtuals WHERE a = 1;
SELECT count() FROM t_ids_virtuals;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_virtuals' AND active AND column = '_row_exists';
INSERT INTO t_ids_virtuals (a, c) VALUES (3, 33);
SELECT a, c FROM t_ids_virtuals ORDER BY a;
DROP TABLE t_ids_virtuals SYNC;

-- why: Nested/Tuple/Map subcolumn streams must resolve through the mapping.
SET flatten_nested = 0;
CREATE TABLE t_ids_complex (n Nested(x UInt64, y String), t Tuple(x UInt64, y String), m Map(String, UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_complex VALUES ([(1, 'a'), (2, 'b')], (3, 'c'), map('k', 4));
SELECT n.x, n.y, t.x, t.y, mapKeys(m), mapValues(m) FROM t_ids_complex;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_complex' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_complex SYNC;
SET flatten_nested = 1;

-- why: merging parts written before and after ADD COLUMN must fill the new column with defaults.
CREATE TABLE t_ids_merge (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_merge VALUES (1, 'one');
INSERT INTO t_ids_merge VALUES (2, 'two');
ALTER TABLE t_ids_merge ADD COLUMN c Nullable(UInt64);
INSERT INTO t_ids_merge (a, b, c) VALUES (3, 'three', 30);
INSERT INTO t_ids_merge (a, b, c) VALUES (4, 'four', 40);
OPTIMIZE TABLE t_ids_merge FINAL;
SELECT * FROM t_ids_merge ORDER BY a;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ids_merge' AND active;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_merge' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_merge SYNC;

-- why: same write/merge path on compact parts.
CREATE TABLE t_ids_compact (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_compact VALUES (1, 'one');
ALTER TABLE t_ids_compact ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_ids_compact (a, b, c) VALUES (2, 'two', 22);
SELECT * FROM t_ids_compact ORDER BY a;
INSERT INTO t_ids_compact (a, b, c) VALUES (3, 'three', 33);
OPTIMIZE TABLE t_ids_compact FINAL;
SELECT * FROM t_ids_compact ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_compact' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_compact SYNC;

-- why: a type-changing mutation must rewrite the counter-ID column in place.
CREATE TABLE t_ids_mutation (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_mutation ADD COLUMN c UInt32 DEFAULT 0;
INSERT INTO t_ids_mutation (a, b, c) VALUES (1, 'one', 10);
INSERT INTO t_ids_mutation (a, b, c) VALUES (2, 'two', 20);
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_mutation' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
ALTER TABLE t_ids_mutation MODIFY COLUMN c UInt64;
SELECT c, toTypeName(c) FROM t_ids_mutation ORDER BY a;
OPTIMIZE TABLE t_ids_mutation FINAL;
SELECT c, toTypeName(c) FROM t_ids_mutation ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_mutation' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_mutation SYNC;

-- why: compact parts must stay readable across a metadata-only RENAME, merge and later ADD.
CREATE TABLE t_ids_compact_rename (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_compact_rename VALUES (1, 'one', 10);
INSERT INTO t_ids_compact_rename VALUES (2, 'two', 20);
ALTER TABLE t_ids_compact_rename RENAME COLUMN b TO d;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_compact_rename' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT a, d, c FROM t_ids_compact_rename ORDER BY a;
INSERT INTO t_ids_compact_rename VALUES (3, 'three', 30);
SELECT a, d, c FROM t_ids_compact_rename ORDER BY a;
OPTIMIZE TABLE t_ids_compact_rename FINAL;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_compact_rename' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT a, d, c FROM t_ids_compact_rename ORDER BY a;
ALTER TABLE t_ids_compact_rename ADD COLUMN e String DEFAULT 'hello';
INSERT INTO t_ids_compact_rename VALUES (4, 'four', 40, 'world');
SELECT a, d, c, e FROM t_ids_compact_rename ORDER BY a;
DROP TABLE t_ids_compact_rename SYNC;

-- why: renaming a non-key column must leave the partition minmax index and pruning intact.
CREATE TABLE t_ids_minmax (a UInt64, b String, dt Date) ENGINE = MergeTree PARTITION BY dt ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_minmax VALUES (1, 'one', '2024-01-01');
INSERT INTO t_ids_minmax VALUES (2, 'two', '2024-01-02');
ALTER TABLE t_ids_minmax RENAME COLUMN b TO d;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_minmax' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT a, d, dt FROM t_ids_minmax ORDER BY a;
SELECT a, d FROM t_ids_minmax WHERE dt = '2024-01-01' ORDER BY a;
INSERT INTO t_ids_minmax VALUES (3, 'three', '2024-01-02');
SELECT a, d FROM t_ids_minmax WHERE dt = '2024-01-02' ORDER BY a;
DROP TABLE t_ids_minmax SYNC;

-- why: serialization.json entries are keyed by column ID; a counter-ID column must land under its ID.
CREATE TABLE t_ids_ser_json (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_ser_json ADD COLUMN c String;
INSERT INTO t_ids_ser_json SELECT number, toString(number), toString(number) FROM numbers(1000);
SELECT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_ser_json' AND active AND column = 'c' LIMIT 1;
DROP TABLE t_ids_ser_json SYNC;

-- why: system.parts_columns shows current logical names with stable column IDs across a metadata-only RENAME.
CREATE TABLE t_ids_partlevel (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_partlevel ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_ids_partlevel VALUES (1, 'one', 10);
SELECT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_partlevel' AND active AND NOT startsWith(column, '_') ORDER BY column;
ALTER TABLE t_ids_partlevel RENAME COLUMN b TO d;
SELECT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_partlevel' AND active AND NOT startsWith(column, '_') ORDER BY column;
DROP TABLE t_ids_partlevel SYNC;

-- why: without opting into column IDs, ALTERs stay on the mutation path.
CREATE TABLE t_ids_no_activate (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_no_activate VALUES (1, 'one');
ALTER TABLE t_ids_no_activate ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_ids_no_activate (a, b, c) VALUES (2, 'two', 22);
ALTER TABLE t_ids_no_activate RENAME COLUMN b TO d;
SELECT count() >= 1 FROM system.mutations WHERE database = currentDatabase() AND table = 't_ids_no_activate';
DROP TABLE t_ids_no_activate SYNC;

-- why: the first RENAME after enabling lazy activation must activate column IDs and stay metadata-only.
CREATE TABLE t_ids_first_rename (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_first_rename VALUES (1, 'one');
ALTER TABLE t_ids_first_rename MODIFY SETTING
    serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_first_rename RENAME COLUMN b TO d;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ids_first_rename' AND NOT is_done;
SELECT a, d FROM t_ids_first_rename ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_first_rename' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_first_rename SYNC;

-- why: the first DROP after enabling lazy activation must activate column IDs and stay metadata-only.
CREATE TABLE t_ids_first_drop (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_first_drop VALUES (1, 'one', 10);
ALTER TABLE t_ids_first_drop MODIFY SETTING
    serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_first_drop DROP COLUMN c;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ids_first_drop' AND NOT is_done;
SELECT * FROM t_ids_first_drop ORDER BY a;
DROP TABLE t_ids_first_drop SYNC;

-- why: mutations must handle compact parts whose columns carry counter IDs.
CREATE TABLE t_ids_compact_mut (a UInt64, b UInt32) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_compact_mut VALUES (1, 10);
INSERT INTO t_ids_compact_mut VALUES (2, 20);
ALTER TABLE t_ids_compact_mut ADD COLUMN c String DEFAULT 'x';
INSERT INTO t_ids_compact_mut VALUES (3, 30, 'y');
ALTER TABLE t_ids_compact_mut MODIFY COLUMN b UInt64;
SELECT a, b, toTypeName(b), c FROM t_ids_compact_mut ORDER BY a;
DROP TABLE t_ids_compact_mut SYNC;

-- why: merging parts from both sides of a RENAME must keep projection parts consistent.
CREATE TABLE t_ids_proj_merge (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_proj_merge ADD PROJECTION p_sum (SELECT a, sum(c) GROUP BY a);
INSERT INTO t_ids_proj_merge VALUES (1, 'one', 10);
INSERT INTO t_ids_proj_merge VALUES (1, 'two', 20);
ALTER TABLE t_ids_proj_merge RENAME COLUMN b TO d;
INSERT INTO t_ids_proj_merge VALUES (2, 'three', 30);
INSERT INTO t_ids_proj_merge VALUES (2, 'four', 40);
OPTIMIZE TABLE t_ids_proj_merge FINAL;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_proj_merge' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT a, sum(c) FROM t_ids_proj_merge GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;
DROP TABLE t_ids_proj_merge SYNC;

-- why: a merge mixing compact and wide inputs must resolve every input's streams by ID.
CREATE TABLE t_ids_mixed_parts (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 100, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_mixed_parts VALUES (1, 'x');
INSERT INTO t_ids_mixed_parts SELECT number, repeat('y', 200) FROM numbers(100, 50);
ALTER TABLE t_ids_mixed_parts ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_ids_mixed_parts VALUES (200, 'z', 42);
OPTIMIZE TABLE t_ids_mixed_parts FINAL;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_mixed_parts' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT count(), sum(c) FROM t_ids_mixed_parts;
DROP TABLE t_ids_mixed_parts SYNC;

-- why: a skip index on a renamed column must keep working across the rename and a merge.
CREATE TABLE t_ids_skip_idx (a UInt64, b String, INDEX idx_b b TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_skip_idx VALUES (1, 'hello world');
INSERT INTO t_ids_skip_idx VALUES (2, 'foo bar');
ALTER TABLE t_ids_skip_idx RENAME COLUMN b TO d;
SELECT a, d FROM t_ids_skip_idx ORDER BY a;
INSERT INTO t_ids_skip_idx VALUES (3, 'baz qux');
OPTIMIZE TABLE t_ids_skip_idx FINAL;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_skip_idx' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT a, d FROM t_ids_skip_idx ORDER BY a;
DROP TABLE t_ids_skip_idx SYNC;

-- why: system.projection_parts_columns must expose column IDs of projection parts.
CREATE TABLE t_ids_proj_sys (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_proj_sys ADD PROJECTION p_sum (SELECT a, sum(c) GROUP BY a);
INSERT INTO t_ids_proj_sys VALUES (1, 'one', 10);
SELECT column, column_id FROM system.projection_parts_columns WHERE database = currentDatabase() AND table = 't_ids_proj_sys' AND active AND NOT startsWith(column, '_') ORDER BY column;
DROP TABLE t_ids_proj_sys SYNC;

-- why: checkDataPart must translate columns.txt names through the mapping, or
-- serialization info entries are dropped and valid parts report corrupted.
CREATE TABLE t_ids_check_ser (a UInt64, b Nullable(String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_check_ser SELECT number, if(number % 10 = 0, toString(number), NULL) FROM numbers(1000);
ALTER TABLE t_ids_check_ser RENAME COLUMN b TO d;
CHECK TABLE t_ids_check_ser SETTINGS check_query_single_value_result = 1;
SELECT a, d FROM t_ids_check_ser WHERE d IS NOT NULL ORDER BY a LIMIT 3;
DROP TABLE t_ids_check_ser SYNC;

-- why: introspection must speak the part's stamped column IDs -- current logical
-- names right after a metadata-only RENAME, ID-based stream filenames, subcolumn
-- sizes from the ID-based streams, and mergeTreeIndex() marks of numeric-ID columns.
CREATE TABLE t_ids_introspection (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_ids_introspection SELECT number, toString(number) FROM numbers(100);
ALTER TABLE t_ids_introspection ADD COLUMN s Nullable(String);
ALTER TABLE t_ids_introspection DROP COLUMN s;
ALTER TABLE t_ids_introspection ADD COLUMN s Nullable(String);
INSERT INTO t_ids_introspection SELECT number, toString(number), concat('x', toString(number)) FROM numbers(100, 100);
ALTER TABLE t_ids_introspection RENAME COLUMN b TO b2;
SELECT name, column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_introspection' AND active
    ORDER BY name, column;
SELECT column, column_id,
       arrayAll(f -> (f != '') AND startsWith(f, '2'), arraySort(filenames)) AS filenames_id_based,
       length(filenames) > 0 AS has_filenames
FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_introspection' AND active
        AND name = 'all_2_2_0' AND column = 's';
SELECT column, subcolumns.names, arrayAll(x -> x > 0, subcolumns.bytes_on_disk) AS subcolumn_sizes_nonzero
FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_introspection' AND active
        AND name = 'all_2_2_0' AND column = 's';
SELECT min(tupleElement(`s.mark`, 1) IS NOT NULL AND tupleElement(`s.mark`, 2) IS NOT NULL) AS marks_real
FROM mergeTreeIndex(currentDatabase(), 't_ids_introspection', with_marks = true)
WHERE part_name = 'all_2_2_0';
-- why (S8-real): column_modification_time must resolve the ID-based stream of a numeric-ID column.
SELECT column, column_modification_time IS NOT NULL AS has_column_modification_time
FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_introspection' AND active
        AND name = 'all_2_2_0' AND column = 's';
-- why (M24): mergeTreeIndex() marks of a column RENAMEd after the part was loaded must
-- resolve through the live mapping -- the part's cached column list still holds the old
-- name, but the ID-keyed stream is unchanged, so the new name's marks must be non-NULL.
SELECT min(tupleElement(`b2.mark`, 1) IS NOT NULL AND tupleElement(`b2.mark`, 2) IS NOT NULL) AS renamed_marks_real
FROM mergeTreeIndex(currentDatabase(), 't_ids_introspection', with_marks = true)
WHERE part_name = 'all_1_1_0';
DROP TABLE t_ids_introspection SYNC;

-- why: the same introspection displays must also speak IDs on COMPACT parts --
-- mergeTreeIndex() marks (M12) and column_modification_time for a numeric-ID column.
CREATE TABLE t_ids_introspection_compact (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000,
    serialization_info_version = 'with_column_ids',
    ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_ids_introspection_compact SELECT number, toString(number) FROM numbers(100);
ALTER TABLE t_ids_introspection_compact ADD COLUMN s Nullable(String);
ALTER TABLE t_ids_introspection_compact DROP COLUMN s;
ALTER TABLE t_ids_introspection_compact ADD COLUMN s Nullable(String);
INSERT INTO t_ids_introspection_compact SELECT number, toString(number), concat('x', toString(number)) FROM numbers(100, 100);
ALTER TABLE t_ids_introspection_compact RENAME COLUMN b TO b2;
SELECT name, column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_introspection_compact' AND active
    ORDER BY name, column;
SELECT min(tupleElement(`s.mark`, 1) IS NOT NULL AND tupleElement(`s.mark`, 2) IS NOT NULL) AS marks_real
FROM mergeTreeIndex(currentDatabase(), 't_ids_introspection_compact', with_marks = true)
WHERE part_name = 'all_2_2_0';
SELECT column, column_modification_time IS NOT NULL AS has_column_modification_time
FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_introspection_compact' AND active
        AND name = 'all_2_2_0' AND column = 's';
DROP TABLE t_ids_introspection_compact SYNC;

-- why (M12 regression): with_marks introspection on a COMPACT part with substream
-- marks must not throw for a streamless-root column -- a Tuple has no root .bin
-- stream, so its root marks come back NULL, while a numeric-ID column still resolves
-- through the ID-keyed substream map and shows real marks.
CREATE TABLE t_ids_introspection_compact_tuple (a UInt64, tup Tuple(x UInt64, y String))
ENGINE = MergeTree ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000,
    serialization_info_version = 'with_column_ids',
    ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_ids_introspection_compact_tuple SELECT number, (number, toString(number)) FROM numbers(100);
ALTER TABLE t_ids_introspection_compact_tuple ADD COLUMN s Nullable(String);
ALTER TABLE t_ids_introspection_compact_tuple DROP COLUMN s;
ALTER TABLE t_ids_introspection_compact_tuple ADD COLUMN s Nullable(String);
INSERT INTO t_ids_introspection_compact_tuple SELECT number, (number, toString(number)), concat('x', toString(number)) FROM numbers(100, 100);
SELECT
    min(tupleElement(`s.mark`, 1) IS NOT NULL) AS s_marks_real,
    max(tupleElement(`tup.mark`, 1) IS NULL) AS tup_root_marks_null
FROM mergeTreeIndex(currentDatabase(), 't_ids_introspection_compact_tuple', with_marks = true)
WHERE part_name = 'all_2_2_0';
DROP TABLE t_ids_introspection_compact_tuple SYNC;
