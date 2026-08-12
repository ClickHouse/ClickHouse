-- Tags: no-parallel, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings, no-object-storage
-- why: continuation of 04048_column_ids_corner_cases -- partition transfer, projections, Nested,
-- empty covering parts, and marks/minmax/byte-size introspection that asserts on local part
-- layout (hence no-object-storage / no-parallel-replicas).  Split to stay under the 180s cap.

SET allow_experimental_column_ids = 1;
SET mutations_sync = 1;

-- why: ATTACH PARTITION FROM succeeds when mappings and counters agree.
CREATE TABLE t_ids_part_src (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
CREATE TABLE t_ids_part_dst (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_part_src ADD COLUMN c UInt64;
ALTER TABLE t_ids_part_src DROP COLUMN c;
ALTER TABLE t_ids_part_dst ADD COLUMN c UInt64;
ALTER TABLE t_ids_part_dst DROP COLUMN c;
INSERT INTO t_ids_part_src VALUES (1, 'hello');
ALTER TABLE t_ids_part_dst ATTACH PARTITION 1 FROM t_ids_part_src;
SELECT a, b FROM t_ids_part_dst;
DROP TABLE t_ids_part_src SYNC;
DROP TABLE t_ids_part_dst SYNC;

-- why: a source counter ahead of the destination is not itself a problem -- what matters is
-- whether a part carries such an ID, and this part was written after the drop, so it does not.
CREATE TABLE t_ids_part_src_b (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
CREATE TABLE t_ids_part_dst_b (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_part_src_b ADD COLUMN c UInt64;
ALTER TABLE t_ids_part_src_b DROP COLUMN c;
INSERT INTO t_ids_part_src_b VALUES (1, 'hello');
ALTER TABLE t_ids_part_dst_b ATTACH PARTITION 1 FROM t_ids_part_src_b;
SELECT a, b FROM t_ids_part_dst_b ORDER BY a;
DROP TABLE t_ids_part_src_b SYNC;
DROP TABLE t_ids_part_dst_b SYNC;

-- why: diverged logical-to-ID mappings (src b->"1" vs dst b->"b") must be rejected.
CREATE TABLE t_ids_part_src2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
CREATE TABLE t_ids_part_dst2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_part_src2 DROP COLUMN b;
ALTER TABLE t_ids_part_src2 ADD COLUMN b String;
INSERT INTO t_ids_part_src2 VALUES (1, 'hello');
ALTER TABLE t_ids_part_dst2 ATTACH PARTITION 1 FROM t_ids_part_src2; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_ids_part_src2 SYNC;
DROP TABLE t_ids_part_dst2 SYNC;

-- why: share_nested_offsets = 0 requests per-column offsets; the ID-based stream name
-- must not fold them back onto the Nested parent prefix.
CREATE TABLE t_ids_nested_no_share (a UInt64, n Nested(x UInt32, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, share_nested_offsets = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_nested_no_share VALUES (1, [10, 20], ['a', 'bb']);
INSERT INTO t_ids_nested_no_share VALUES (2, [30], ['cc']);
SELECT a, n.x, n.y FROM t_ids_nested_no_share ORDER BY a;
OPTIMIZE TABLE t_ids_nested_no_share FINAL;
SELECT a, n.x, n.y FROM t_ids_nested_no_share ORDER BY a;
DROP TABLE t_ids_nested_no_share SYNC;

-- why: projection parts must survive a parent-column rename and a merge.
CREATE TABLE t_ids_proj (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_proj ADD PROJECTION p_sum (SELECT a, sum(c) GROUP BY a);
INSERT INTO t_ids_proj VALUES (1, 'one', 10);
INSERT INTO t_ids_proj VALUES (1, 'two', 20);
INSERT INTO t_ids_proj VALUES (2, 'three', 30);
SELECT a, sum(c) FROM t_ids_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;
ALTER TABLE t_ids_proj RENAME COLUMN b TO d;
SELECT a, sum(c) FROM t_ids_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;
OPTIMIZE TABLE t_ids_proj FINAL;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_proj' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT a, sum(c) FROM t_ids_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;
DROP TABLE t_ids_proj SYNC;

-- why: a flattened Nested ADDed after activation gets compound column IDs, one counter per child
-- under the group's shared prefix ("1.2", "1.3").
CREATE TABLE t_ids_flat_add (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_flat_add VALUES (1, 'one');
ALTER TABLE t_ids_flat_add ADD COLUMN n Nested(x UInt64, y String);
INSERT INTO t_ids_flat_add VALUES (2, 'two', [10, 20], ['a', 'b']);
INSERT INTO t_ids_flat_add VALUES (3, 'three', [30], ['c']);
SELECT a, b, `n.x`, `n.y` FROM t_ids_flat_add ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_flat_add' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;
OPTIMIZE TABLE t_ids_flat_add FINAL;
SELECT a, b, `n.x`, `n.y` FROM t_ids_flat_add ORDER BY a;
DROP TABLE t_ids_flat_add SYNC;

-- why: renaming a field WITHIN a Nested group is metadata-only and keeps the compound ID.
CREATE TABLE t_ids_flat_rename (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_flat_rename ADD COLUMN n Nested(x UInt64, y String);
INSERT INTO t_ids_flat_rename VALUES (1, 'hello', [10, 20], ['a', 'b']);
INSERT INTO t_ids_flat_rename VALUES (2, 'world', [30], ['c']);
ALTER TABLE t_ids_flat_rename RENAME COLUMN `n.x` TO `n.z`;
SELECT a, b, `n.z`, `n.y` FROM t_ids_flat_rename ORDER BY a;
OPTIMIZE TABLE t_ids_flat_rename FINAL;
SELECT a, b, `n.z`, `n.y` FROM t_ids_flat_rename ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_flat_rename' AND active
    AND column LIKE 'n.%'
    ORDER BY column, column_id;
DROP TABLE t_ids_flat_rename SYNC;

-- why: a pre-activation flattened Nested keeps identity IDs after lazy activation.
CREATE TABLE t_ids_flat_existing (a UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_flat_existing VALUES (1, [10, 20], ['a', 'b']);
INSERT INTO t_ids_flat_existing VALUES (2, [30], ['c']);
ALTER TABLE t_ids_flat_existing MODIFY SETTING
    serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_flat_existing ADD COLUMN c UInt64 DEFAULT 0;
SELECT DISTINCT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_flat_existing' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;
ALTER TABLE t_ids_flat_existing RENAME COLUMN c TO d;
SELECT a, `n.x`, `n.y`, d FROM t_ids_flat_existing ORDER BY a;
OPTIMIZE TABLE t_ids_flat_existing FINAL;
SELECT a, `n.x`, `n.y`, d FROM t_ids_flat_existing ORDER BY a;
DROP TABLE t_ids_flat_existing SYNC;

-- why: non-flattened Nested (flatten_nested = 0) must survive a sibling rename and merge.
SET flatten_nested = 0;
CREATE TABLE t_ids_nested_nf (a UInt64, b String, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_nested_nf VALUES (1, 'hello', [(10, 'a'), (20, 'b')]);
INSERT INTO t_ids_nested_nf VALUES (2, 'world', [(30, 'c')]);
ALTER TABLE t_ids_nested_nf RENAME COLUMN b TO d;
SELECT a, d, n.x, n.y FROM t_ids_nested_nf ORDER BY a;
OPTIMIZE TABLE t_ids_nested_nf FINAL;
SELECT a, d, n.x, n.y FROM t_ids_nested_nf ORDER BY a;
DROP TABLE t_ids_nested_nf SYNC;
SET flatten_nested = 1;

-- why: identity-mapped Nested children may move across parents as metadata-only -- the
-- offset stream name derives from the physical prefix, which the rename does not change.
CREATE TABLE t_ids_identity_nested_rename (a UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_identity_nested_rename VALUES (1, [10, 20], ['a', 'b']);
INSERT INTO t_ids_identity_nested_rename VALUES (2, [30], ['c']);
ALTER TABLE t_ids_identity_nested_rename MODIFY SETTING
    serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_identity_nested_rename ADD COLUMN c UInt64 DEFAULT 0;
SELECT DISTINCT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_identity_nested_rename' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;
ALTER TABLE t_ids_identity_nested_rename RENAME COLUMN `n.x` TO `m.x`, RENAME COLUMN `n.y` TO `m.y`;
SELECT a, `m.x`, `m.y` FROM t_ids_identity_nested_rename ORDER BY a;
OPTIMIZE TABLE t_ids_identity_nested_rename FINAL;
SELECT a, `m.x`, `m.y` FROM t_ids_identity_nested_rename ORDER BY a;
DROP TABLE t_ids_identity_nested_rename SYNC;

-- why: a live column named like another live column's ID would make on-disk keys
-- ambiguous; the stream-collision check must reject every ALTER route to that state.
CREATE TABLE t_ids_name_id_guard (a UInt64, b UInt64, x UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_name_id_guard RENAME COLUMN b TO d; -- d keeps identity ID 'b'
ALTER TABLE t_ids_name_id_guard ADD COLUMN b UInt64; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_ids_name_id_guard RENAME COLUMN x TO b; -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_ids_name_id_guard' ORDER BY name;
DROP TABLE t_ids_name_id_guard SYNC;


-- ===== empty covering parts (DETACH/DROP PART, DROP PARTITION, TRUNCATE) must be id-keyed =====
-- Before the fix, createEmptyPart wrote the covering part's columns.txt from the current logical
-- schema without stamping ids, so system.parts_columns.column_id reported the logical name (e.g.
-- 'c1') instead of the stable id ('1'). Large old_parts_lifetime keeps the transient Outdated
-- covering part observable so the assertion is not racing GC.

-- DETACH PART
DROP TABLE IF EXISTS t_detach SYNC;
CREATE TABLE t_detach (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;
INSERT INTO t_detach VALUES (1);
ALTER TABLE t_detach ADD COLUMN c UInt64 DEFAULT 7;
INSERT INTO t_detach (a, c) VALUES (2, 20);
ALTER TABLE t_detach RENAME COLUMN c TO c1;
ALTER TABLE t_detach DETACH PART 'all_2_2_0';
SELECT 'detach_part', arraySort(groupArrayDistinct(column_id))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_detach' AND column = 'c1';

-- DROP PART
DROP TABLE IF EXISTS t_drop SYNC;
CREATE TABLE t_drop (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;
INSERT INTO t_drop VALUES (1);
ALTER TABLE t_drop ADD COLUMN c UInt64 DEFAULT 7;
INSERT INTO t_drop (a, c) VALUES (2, 20);
ALTER TABLE t_drop RENAME COLUMN c TO c1;
ALTER TABLE t_drop DROP PART 'all_2_2_0';
SELECT 'drop_part', arraySort(groupArrayDistinct(column_id))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_drop' AND column = 'c1';

-- DROP PARTITION
DROP TABLE IF EXISTS t_droppart SYNC;
CREATE TABLE t_droppart (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;
INSERT INTO t_droppart VALUES (1);
ALTER TABLE t_droppart ADD COLUMN c UInt64 DEFAULT 7;
INSERT INTO t_droppart (a, c) VALUES (2, 20);
ALTER TABLE t_droppart RENAME COLUMN c TO c1;
ALTER TABLE t_droppart DROP PARTITION tuple();
SELECT 'drop_partition', arraySort(groupArrayDistinct(column_id))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_droppart' AND column = 'c1';

-- TRUNCATE
DROP TABLE IF EXISTS t_trunc SYNC;
CREATE TABLE t_trunc (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;
INSERT INTO t_trunc VALUES (1);
ALTER TABLE t_trunc ADD COLUMN c UInt64 DEFAULT 7;
INSERT INTO t_trunc (a, c) VALUES (2, 20);
ALTER TABLE t_trunc RENAME COLUMN c TO c1;
TRUNCATE TABLE t_trunc;
SELECT 'truncate', arraySort(groupArrayDistinct(column_id))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_trunc' AND column = 'c1';

DROP TABLE t_detach SYNC;
DROP TABLE t_drop SYNC;
DROP TABLE t_droppart SYNC;
DROP TABLE t_trunc SYNC;


-- ===== CLEAR COLUMN must reset a renamed column to its default =====
-- The part keeps its load-time name after a metadata-only RENAME, so a name-resolved file drop
-- missed the id-keyed file, leaving the data intact (a silent no-op). The fix resolves the part's
-- column by its physical id. Covered for an added (id-keyed) column and an original (id == name).
SET mutations_sync = 2;
DROP TABLE IF EXISTS cv SYNC;
CREATE TABLE cv (k UInt64) ENGINE = MergeTree PARTITION BY k ORDER BY k
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0;
ALTER TABLE cv ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO cv VALUES (1, 111);
ALTER TABLE cv RENAME COLUMN c TO c1;
ALTER TABLE cv CLEAR COLUMN c1 IN PARTITION 1;
SELECT 'clear_added_renamed', c1 FROM cv;

DROP TABLE IF EXISTS co SYNC;
CREATE TABLE co (k UInt64, c UInt64) ENGINE = MergeTree PARTITION BY k ORDER BY k
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0;
INSERT INTO co VALUES (1, 111);
ALTER TABLE co RENAME COLUMN c TO c1;
ALTER TABLE co CLEAR COLUMN c1 IN PARTITION 1;
SELECT 'clear_original_renamed', c1 FROM co;
DROP TABLE cv SYNC;
DROP TABLE co SYNC;


-- ===== rename onto a name freed by an earlier rename/drop: orphan stream keeps its own slot =====
-- On reload the part's physical column list must resolve every key in ID-space: the live column
-- keeps the name, the orphan keeps its slot under its own key -- never re-bound to the live
-- sibling's stream. A regression here made the reloaded part a duplicate-column error.

-- Two-column rotation is rejected: after a -> x, column x holds id 'a', so renaming b onto 'a'
-- would make a logical name equal to another active column's id. It must fail loudly.
DROP TABLE IF EXISTS t_rot;
CREATE TABLE t_rot (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_rot VALUES (1, 10, 20);
ALTER TABLE t_rot MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_rot RENAME COLUMN a TO x;
ALTER TABLE t_rot RENAME COLUMN b TO a; -- { serverError BAD_ARGUMENTS }

-- DROP then RENAME the survivor onto the dropped name -- Wide and Compact, both the last-slot
-- orphan (DROP b) and the middle-slot orphan (DROP a).
DROP TABLE IF EXISTS t_dropb_wide;
CREATE TABLE t_dropb_wide (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_dropb_wide VALUES (1, 10, 20);
ALTER TABLE t_dropb_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_dropb_wide DROP COLUMN b;
ALTER TABLE t_dropb_wide RENAME COLUMN a TO b;
DETACH TABLE t_dropb_wide;
ATTACH TABLE t_dropb_wide;
SELECT 'dropb_wide', k, b FROM t_dropb_wide;

DROP TABLE IF EXISTS t_dropa_wide;
CREATE TABLE t_dropa_wide (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_dropa_wide VALUES (1, 10, 20);
ALTER TABLE t_dropa_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_dropa_wide DROP COLUMN a;
ALTER TABLE t_dropa_wide RENAME COLUMN b TO a;
DETACH TABLE t_dropa_wide;
ATTACH TABLE t_dropa_wide;
SELECT 'dropa_wide', k, a FROM t_dropa_wide;

DROP TABLE IF EXISTS t_dropb_compact;
CREATE TABLE t_dropb_compact (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_dropb_compact VALUES (1, 10, 20);
ALTER TABLE t_dropb_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_dropb_compact DROP COLUMN b;
ALTER TABLE t_dropb_compact RENAME COLUMN a TO b;
DETACH TABLE t_dropb_compact;
ATTACH TABLE t_dropb_compact;
SELECT 'dropb_compact', k, b FROM t_dropb_compact;

DROP TABLE IF EXISTS t_dropa_compact;
CREATE TABLE t_dropa_compact (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_dropa_compact VALUES (1, 10, 20);
ALTER TABLE t_dropa_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_dropa_compact DROP COLUMN a;
ALTER TABLE t_dropa_compact RENAME COLUMN b TO a;
DETACH TABLE t_dropa_compact;
ATTACH TABLE t_dropa_compact;
SELECT 'dropa_compact', k, a FROM t_dropa_compact;

-- DROP then re-ADD the same name: the reused name gets a fresh ID absent from the old part, so the
-- orphan is not re-adopted and the reader default-fills.
DROP TABLE IF EXISTS t_readd;
CREATE TABLE t_readd (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_readd VALUES (1, 99);
ALTER TABLE t_readd MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_readd DROP COLUMN a;
ALTER TABLE t_readd ADD COLUMN a UInt64 DEFAULT 7;
DETACH TABLE t_readd;
ATTACH TABLE t_readd;
SELECT 'readd', k, a FROM t_readd;
DROP TABLE t_rot;
DROP TABLE t_dropb_wide;
DROP TABLE t_dropa_wide;
DROP TABLE t_dropb_compact;
DROP TABLE t_dropa_compact;
DROP TABLE t_readd;


-- ===== DROP + re-ADD: marks introspection must resolve the part column by id, not name =====
-- The re-added column gets a fresh id absent from the old part, whose original orphan stream still
-- sits on disk under the old id. The marks path must resolve by id, else it binds the orphan
-- stream and reports its marks for a column absent from the part. Only the marks path is observable.
DROP TABLE IF EXISTS t_readd_marks_wide;
CREATE TABLE t_readd_marks_wide (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_readd_marks_wide VALUES (1, 99);
ALTER TABLE t_readd_marks_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_readd_marks_wide DROP COLUMN a;
ALTER TABLE t_readd_marks_wide ADD COLUMN a UInt64 DEFAULT 7;
DETACH TABLE t_readd_marks_wide;
ATTACH TABLE t_readd_marks_wide;
SELECT 'wide_data', k, a FROM t_readd_marks_wide;
SELECT 'wide_readd_marks_null',
       min(tupleElement(`a.mark`, 1) IS NULL AND tupleElement(`a.mark`, 2) IS NULL) AS marks_null
FROM mergeTreeIndex(currentDatabase(), 't_readd_marks_wide', with_marks = true)
WHERE part_name = 'all_1_1_0';

DROP TABLE IF EXISTS t_readd_marks_compact;
CREATE TABLE t_readd_marks_compact (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_readd_marks_compact VALUES (1, 99);
ALTER TABLE t_readd_marks_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_readd_marks_compact DROP COLUMN a;
ALTER TABLE t_readd_marks_compact ADD COLUMN a UInt64 DEFAULT 7;
DETACH TABLE t_readd_marks_compact;
ATTACH TABLE t_readd_marks_compact;
SELECT 'compact_data', k, a FROM t_readd_marks_compact;
SELECT 'compact_readd_marks_null',
       min(tupleElement(`a.mark`, 1) IS NULL AND tupleElement(`a.mark`, 2) IS NULL) AS marks_null
FROM mergeTreeIndex(currentDatabase(), 't_readd_marks_compact', with_marks = true)
WHERE part_name = 'all_1_1_0';
DROP TABLE t_readd_marks_wide;
DROP TABLE t_readd_marks_compact;


-- ===== orphan placeholder collision: a fresh-id column absent from the part must default-fill =====
-- DROP b + RENAME a TO b makes the orphan's stale name 'b' collide with the live 'b'; the orphan is
-- renamed to a unique placeholder so the part's column list has no duplicate name. A later 'b_' is a
-- real column absent from the old part -- its marks must be NULL (fillMarks resolves by stable id).
DROP TABLE IF EXISTS t_orphan_collide_wide;
CREATE TABLE t_orphan_collide_wide (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_orphan_collide_wide VALUES (1, 10, 20);
ALTER TABLE t_orphan_collide_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_orphan_collide_wide DROP COLUMN b;
ALTER TABLE t_orphan_collide_wide RENAME COLUMN a TO b;
ALTER TABLE t_orphan_collide_wide ADD COLUMN `b_` UInt64 DEFAULT 7;
DETACH TABLE t_orphan_collide_wide;
ATTACH TABLE t_orphan_collide_wide;
SELECT 'wide_data', k, b, `b_` FROM t_orphan_collide_wide;
SELECT 'wide_absent_marks_null',
       min(tupleElement(`b_.mark`, 1) IS NULL AND tupleElement(`b_.mark`, 2) IS NULL) AS marks_null
FROM mergeTreeIndex(currentDatabase(), 't_orphan_collide_wide', with_marks = true)
WHERE part_name = 'all_1_1_0';

DROP TABLE IF EXISTS t_orphan_collide_compact;
CREATE TABLE t_orphan_collide_compact (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_orphan_collide_compact VALUES (1, 10, 20);
ALTER TABLE t_orphan_collide_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_orphan_collide_compact DROP COLUMN b;
ALTER TABLE t_orphan_collide_compact RENAME COLUMN a TO b;
ALTER TABLE t_orphan_collide_compact ADD COLUMN `b_` UInt64 DEFAULT 7;
DETACH TABLE t_orphan_collide_compact;
ATTACH TABLE t_orphan_collide_compact;
SELECT 'compact_data', k, b, `b_` FROM t_orphan_collide_compact;
SELECT 'compact_absent_marks_null',
       min(tupleElement(`b_.mark`, 1) IS NULL AND tupleElement(`b_.mark`, 2) IS NULL) AS marks_null
FROM mergeTreeIndex(currentDatabase(), 't_orphan_collide_compact', with_marks = true)
WHERE part_name = 'all_1_1_0';
DROP TABLE t_orphan_collide_wide;
DROP TABLE t_orphan_collide_compact;


-- ===== minmax partition pruning must resolve each part's minmax file by the partition-key id =====
-- MinMaxIndex::load and the checkConsistencyBase minmax checks resolve id-first, so a foreign
-- column's minmax_<id>.idx can never bind to a different partition-key column. Mapping churn
-- (ADD/DROP of a non-partition column) keeps a non-trivial live mapping across a part reload.
DROP TABLE IF EXISTS mm_wide;
CREATE TABLE mm_wide (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO mm_wide VALUES (1, 10, 100), (2, 10, 100), (3, 20, 200), (4, 30, 300);
ALTER TABLE mm_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE mm_wide ADD COLUMN c UInt64 DEFAULT 0;
ALTER TABLE mm_wide DROP COLUMN c;
ALTER TABLE mm_wide ADD COLUMN d UInt64 DEFAULT 0;
INSERT INTO mm_wide VALUES (5, 10, 100, 9), (6, 40, 400, 9);
OPTIMIZE TABLE mm_wide FINAL;
DETACH TABLE mm_wide;
ATTACH TABLE mm_wide;
SELECT 'wide_a10', k, a, b, d FROM mm_wide WHERE a = 10 ORDER BY k;
SELECT 'wide_b300', k FROM mm_wide WHERE b = 300 ORDER BY k;
SELECT 'wide_a40_b400', k FROM mm_wide WHERE a = 40 AND b = 400 ORDER BY k;
SELECT 'wide_total', count() FROM mm_wide;

DROP TABLE IF EXISTS mm_compact;
CREATE TABLE mm_compact (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO mm_compact VALUES (1, 10, 100), (2, 10, 100), (3, 20, 200), (4, 30, 300);
ALTER TABLE mm_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE mm_compact ADD COLUMN c UInt64 DEFAULT 0;
ALTER TABLE mm_compact DROP COLUMN c;
ALTER TABLE mm_compact ADD COLUMN d UInt64 DEFAULT 0;
INSERT INTO mm_compact VALUES (5, 10, 100, 9), (6, 40, 400, 9);
OPTIMIZE TABLE mm_compact FINAL;
DETACH TABLE mm_compact;
ATTACH TABLE mm_compact;
SELECT 'compact_a10', k, a, b, d FROM mm_compact WHERE a = 10 ORDER BY k;
SELECT 'compact_b300', k FROM mm_compact WHERE b = 300 ORDER BY k;
SELECT 'compact_a40_b400', k FROM mm_compact WHERE a = 40 AND b = 400 ORDER BY k;
SELECT 'compact_total', count() FROM mm_compact;
DROP TABLE mm_wide;
DROP TABLE mm_compact;


-- ===== orphan column-size attribution: an orphan whose id-token equals a live name must be skipped =====
-- DROP a; ADD a: the re-added 'a' gets a fresh id, and the old part's original 'a' stream becomes an
-- orphan whose stamped id equals the new live column's logical name 'a'. The column-size aggregate
-- keys live columns by name and orphans by id-token (both plain strings that can coincide), so an
-- orphan with no live logical name must be skipped, else its bytes are attributed to the live 'a'.
DROP TABLE IF EXISTS t_orphan_size;
CREATE TABLE t_orphan_size (k UInt64, a String) ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_orphan_size SELECT number, toString(rand64()) || toString(rand64()) FROM numbers(20000);
SELECT 'dropped_a_has_bytes', data_compressed_bytes > 0 AS big
FROM system.columns WHERE database = currentDatabase() AND table = 't_orphan_size' AND name = 'a';
ALTER TABLE t_orphan_size DROP COLUMN a;
ALTER TABLE t_orphan_size ADD COLUMN a String;
DETACH TABLE t_orphan_size;
ATTACH TABLE t_orphan_size;
SELECT 'live_a_size_zero', data_compressed_bytes = 0 AS is_zero
FROM system.columns WHERE database = currentDatabase() AND table = 't_orphan_size' AND name = 'a';
DROP TABLE t_orphan_size;

-- ===== a mutation over a part holding both a dropped name and a rename onto it =====
-- DROP q; RENAME p TO q, two metadata-only ALTERs, leave the part carrying both the dropped q
-- (Int64) and the renamed p (String) -- see the two `q` rows below. splitAndModifyMutationCommands
-- brings the part's load-time names into the current schema by id, and must evict the dropped q
-- before renaming p onto it: the part's column list is name-unique, so a rename onto a live name
-- silently drops the renamed column and leaves the dropped one -- q would read back Int64.
-- Both part types, because the split branches on isWidePart.
DROP TABLE IF EXISTS t_mutate_over_freed_wide;
DROP TABLE IF EXISTS t_mutate_over_freed_compact;
CREATE TABLE t_mutate_over_freed_wide (k UInt64, p String, q Int64, n Int32) ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_mutate_over_freed_compact (k UInt64, p String, q Int64, n Int32) ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 10000000, min_rows_for_wide_part = 10000000;
INSERT INTO t_mutate_over_freed_wide VALUES (1, 'kept', -7, 100);
INSERT INTO t_mutate_over_freed_compact VALUES (1, 'kept', -7, 100);
ALTER TABLE t_mutate_over_freed_wide DROP COLUMN q, RENAME COLUMN p TO q; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_mutate_over_freed_wide DROP COLUMN q;
ALTER TABLE t_mutate_over_freed_wide RENAME COLUMN p TO q;
ALTER TABLE t_mutate_over_freed_compact DROP COLUMN q;
ALTER TABLE t_mutate_over_freed_compact RENAME COLUMN p TO q;
SELECT 'freed: two q in the part', part_type, column, column_id, type FROM system.parts_columns
WHERE database = currentDatabase() AND table LIKE 't_mutate_over_freed_%' AND active AND column = 'q'
ORDER BY part_type, column_id;
ALTER TABLE t_mutate_over_freed_wide UPDATE n = n + 1 WHERE 1;
ALTER TABLE t_mutate_over_freed_compact UPDATE n = n + 1 WHERE 1;
SELECT 'freed: wide after mutation', k, q, n FROM t_mutate_over_freed_wide ORDER BY k;
SELECT 'freed: compact after mutation', k, q, n FROM t_mutate_over_freed_compact ORDER BY k;
-- A mutation OF the renamed column itself, whose command names it by its current name.
ALTER TABLE t_mutate_over_freed_wide UPDATE q = concat(q, '!') WHERE 1;
ALTER TABLE t_mutate_over_freed_compact UPDATE q = concat(q, '!') WHERE 1;
SELECT 'freed: wide q mutated', k, q FROM t_mutate_over_freed_wide ORDER BY k;
SELECT 'freed: compact q mutated', k, q FROM t_mutate_over_freed_compact ORDER BY k;
-- The rewrite drops the evicted column; only the renamed one, under its own id, is left.
SELECT 'freed: one q left', part_type, column, column_id, type FROM system.parts_columns
WHERE database = currentDatabase() AND table LIKE 't_mutate_over_freed_%' AND active AND column = 'q'
ORDER BY part_type, column_id;
DROP TABLE t_mutate_over_freed_wide;
DROP TABLE t_mutate_over_freed_compact;
