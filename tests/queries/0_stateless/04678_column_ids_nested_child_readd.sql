-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings, no-object-storage
-- why: a flattened Nested child's compound column ID must not be reconstructible from its logical
-- name. DROP of a child in one ALTER and ADD of the same name in a LATER one is not covered by the
-- single-ALTER rejection, so a name-derived ID would rebuild itself and bind the re-added column to
-- the dropped column's streams. The counter half makes the ID unique while the prefix keeps the
-- group on one offsets stream. The tags pin part layout: the assertions name stream files.

SET allow_experimental_column_ids = 1;

-- why: a re-added child beside a live sibling. The prefix is inherited from that sibling, so the
-- fresh ID differs from the dropped one only in its counter half, and both children keep sharing
-- the group's single offsets stream.
CREATE TABLE t_readd_child (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_readd_child ADD COLUMN `n.x` Array(UInt64), ADD COLUMN `n.y` Array(String);
INSERT INTO t_readd_child (k, `n.x`, `n.y`) VALUES (1, [111, 222], ['keep', 'keep2']);
ALTER TABLE t_readd_child DROP COLUMN `n.x`;
ALTER TABLE t_readd_child ADD COLUMN `n.x` Array(UInt64);
INSERT INTO t_readd_child (k, `n.x`, `n.y`) VALUES (2, [333], ['fresh']);

SELECT 'sibling: ids of n.x', arraySort(groupUniqArray(column_id)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_child' AND active AND column = 'n.x';
SELECT 'sibling: ids of n.y', arraySort(groupUniqArray(column_id)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_child' AND active AND column = 'n.y';
SELECT 'sibling: offsets streams', arraySort(groupUniqArray(arrayJoin(arrayFilter(f -> position(f, 'size0') > 0, filenames))))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_child' AND active AND column LIKE 'n.%';
-- The re-added child reads its DEFAULT, filled to the group's array length in the old part.
SELECT 'sibling: rows', k, `n.x`, `n.y` FROM t_readd_child ORDER BY k;
OPTIMIZE TABLE t_readd_child FINAL;
SELECT 'sibling: rows after merge', k, `n.x`, `n.y` FROM t_readd_child ORDER BY k;
DROP TABLE t_readd_child SYNC;

-- why: the group's only child. There is no sibling to inherit a prefix from, so the whole compound
-- ID is fresh -- prefix included -- and the re-added child reads through its own offsets stream.
CREATE TABLE t_readd_only_child (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_readd_only_child ADD COLUMN `n.x` Array(UInt64);
INSERT INTO t_readd_only_child (k, `n.x`) VALUES (1, [111, 222]);
ALTER TABLE t_readd_only_child DROP COLUMN `n.x`;
ALTER TABLE t_readd_only_child ADD COLUMN `n.x` Array(UInt64);
INSERT INTO t_readd_only_child (k, `n.x`) VALUES (2, [333]);

SELECT 'only child: ids of n.x', arraySort(groupUniqArray(column_id)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_only_child' AND active AND column = 'n.x';
SELECT 'only child: offsets streams', arraySort(groupUniqArray(arrayJoin(arrayFilter(f -> position(f, 'size0') > 0, filenames))))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_only_child' AND active AND column LIKE 'n.%';
SELECT 'only child: rows', k, `n.x` FROM t_readd_only_child ORDER BY k;
OPTIMIZE TABLE t_readd_only_child FINAL;
SELECT 'only child: rows after merge', k, `n.x` FROM t_readd_only_child ORDER BY k;
DROP TABLE t_readd_only_child SYNC;

-- why: a group whose IDs are the identity form -- children that existed when the mapping was
-- created, so their ID is their own name. The dropped child's ID is the most reconstructible shape
-- there is, and the fresh one has to differ from it while keeping the identity prefix's stream.
CREATE TABLE t_readd_identity (k UInt64, `n.x` Array(UInt64), `n.y` Array(String))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_readd_identity (k, `n.x`, `n.y`) VALUES (1, [111, 222], ['keep', 'keep2']);
ALTER TABLE t_readd_identity DROP COLUMN `n.x`;
ALTER TABLE t_readd_identity ADD COLUMN `n.x` Array(UInt64);
INSERT INTO t_readd_identity (k, `n.x`, `n.y`) VALUES (2, [333], ['fresh']);

SELECT 'identity: ids of n.x', arraySort(groupUniqArray(column_id)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_identity' AND active AND column = 'n.x';
SELECT 'identity: ids of n.y', arraySort(groupUniqArray(column_id)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_identity' AND active AND column = 'n.y';
SELECT 'identity: offsets streams', arraySort(groupUniqArray(arrayJoin(arrayFilter(f -> position(f, 'size0') > 0, filenames))))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_identity' AND active AND column LIKE 'n.%';
SELECT 'identity: rows', k, `n.x`, `n.y` FROM t_readd_identity ORDER BY k;
OPTIMIZE TABLE t_readd_identity FINAL;
SELECT 'identity: rows after merge', k, `n.x`, `n.y` FROM t_readd_identity ORDER BY k;
SELECT 'identity: ids after merge', arraySort(groupUniqArray(column_id)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_identity' AND active AND column = 'n.x';
DROP TABLE t_readd_identity SYNC;
