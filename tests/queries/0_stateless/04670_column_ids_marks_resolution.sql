-- Tags: no-random-merge-tree-settings, no-object-storage, no-parallel-replicas
-- why: `getColumnMarksLocation` answers a marks question per part format and fails closed on a
-- column with no stamped ID. Each case below asks it for a stream the part does not keep, or for
-- a column the mapping never covers, on a table whose parts are keyed by column IDs.
-- The tags pin part layout: the assertions are about which marks a specific part holds.

SET allow_experimental_column_ids = 1;
SET mutations_sync = 2;

-- why: a Tuple root has no stream of its own, so a Compact part with substream marks holds no
-- slot for it -- NULL marks, not the slot of whatever substream sits at the root's position.
CREATE TABLE t_ids_marks_tuple (k UInt64, t Tuple(x UInt64, y String)) ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 1, index_granularity = 1;
-- Push t off identity so its streams are named by the ID, not by 't'.
ALTER TABLE t_ids_marks_tuple DROP COLUMN t;
ALTER TABLE t_ids_marks_tuple ADD COLUMN t Tuple(x UInt64, y String);
INSERT INTO t_ids_marks_tuple VALUES (1, (10, 'a')), (2, (20, 'b'));
SELECT 'tuple_id_non_identity', column_id != column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ids_marks_tuple' AND active AND column = 't';
SELECT 'tuple_root_marks_null',
       min(tupleElement(`t.mark`, 1) IS NULL AND tupleElement(`t.mark`, 2) IS NULL),
       min(tupleElement(`t%2Ex.mark`, 1) IS NOT NULL),
       min(tupleElement(`t%2Ey.mark`, 1) IS NOT NULL),
       min(tupleElement(`k.mark`, 1) IS NOT NULL)
FROM mergeTreeIndex(currentDatabase(), 't_ids_marks_tuple', with_marks = true)
WHERE part_name = 'all_1_1_0';
DROP TABLE t_ids_marks_tuple SYNC;

-- why: the same question on a Wide part for a column added after it was written -- the part keeps
-- no stream under that column's ID, so its marks are NULL while its siblings' resolve.
CREATE TABLE t_ids_marks_wide (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_ids_marks_wide VALUES (1, 10), (2, 20);
ALTER TABLE t_ids_marks_wide ADD COLUMN b UInt64 DEFAULT 7;
SELECT 'wide_late_column_marks_null',
       min(tupleElement(`b.mark`, 1) IS NULL AND tupleElement(`b.mark`, 2) IS NULL),
       min(tupleElement(`a.mark`, 1) IS NOT NULL),
       min(tupleElement(`k.mark`, 1) IS NOT NULL)
FROM mergeTreeIndex(currentDatabase(), 't_ids_marks_wide', with_marks = true)
WHERE part_name = 'all_1_1_0';
DROP TABLE t_ids_marks_wide SYNC;

-- why: `_row_exists` is persistent but never in the mapping, so a part carrying it is stamped
-- everywhere else and unstamped there. Asking for its marks must take the virtual-column exemption
-- instead of the fail-closed `LOGICAL_ERROR`. `columns_to_prewarm_mark_cache` is what gets the
-- prewarm to ask; dropping the table asks again on the way out.
CREATE TABLE t_ids_marks_row_exists (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1,
         columns_to_prewarm_mark_cache = '_row_exists';
ALTER TABLE t_ids_marks_row_exists DROP COLUMN a;
ALTER TABLE t_ids_marks_row_exists ADD COLUMN a UInt64;
INSERT INTO t_ids_marks_row_exists VALUES (1, 10), (2, 20), (3, 30);
DELETE FROM t_ids_marks_row_exists WHERE k = 2;
SELECT 'row_exists_in_part', has_lightweight_delete
FROM system.parts WHERE database = currentDatabase() AND table = 't_ids_marks_row_exists' AND active;
SYSTEM PREWARM MARK CACHE t_ids_marks_row_exists;
SELECT 'row_exists_prewarm_ok';
SELECT 'row_exists_data', k, a FROM t_ids_marks_row_exists ORDER BY k;
SELECT 'row_exists_sibling_marks',
       min(tupleElement(`a.mark`, 1) IS NOT NULL),
       min(tupleElement(`k.mark`, 1) IS NOT NULL)
FROM mergeTreeIndex(currentDatabase(), 't_ids_marks_row_exists', with_marks = true);
DROP TABLE t_ids_marks_row_exists SYNC;
