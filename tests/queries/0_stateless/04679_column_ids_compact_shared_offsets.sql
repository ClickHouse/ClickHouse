-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- why: a Compact part resolves a missing Array column's offsets through findColumnForOffsets, which
-- has to ask WHICH COLUMNS SHARE AN OFFSETS STREAM -- an id-space question, since the stream is named
-- from the id's Nested prefix. Asking in logical-name space got two shapes wrong in opposite
-- directions: a re-added name matched the dropped column still in the part and read arrays of ITS
-- length, and a sibling added after a cross-parent rename matched nothing, so it did not share the
-- group's stream at all. The tags pin part type; Wide reads siblings by another path.

SET allow_experimental_column_ids = 1;

-- why: DROP is metadata-only, so the dropped `a` stays in the part under its own id. The re-added
-- `a` has a fresh id and must read an EMPTY array -- what Wide and a table without column IDs give.
CREATE TABLE t_reoffset_dropped (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 10000000, serialization_info_version = 'with_column_ids';
INSERT INTO t_reoffset_dropped VALUES (1, [111, 222]);
ALTER TABLE t_reoffset_dropped DROP COLUMN a;
ALTER TABLE t_reoffset_dropped ADD COLUMN a Array(UInt64);
SELECT 'dropped: part type', any(part_type) FROM system.parts
WHERE database = currentDatabase() AND table = 't_reoffset_dropped' AND active;
SELECT 'dropped: re-added a', k, a, length(a) FROM t_reoffset_dropped ORDER BY k;

-- why: the same DDL on a Wide part, which resolves siblings elsewhere -- the control that says what
-- the answer above should be.
CREATE TABLE t_reoffset_wide (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         serialization_info_version = 'with_column_ids';
INSERT INTO t_reoffset_wide VALUES (1, [111, 222]);
ALTER TABLE t_reoffset_wide DROP COLUMN a;
ALTER TABLE t_reoffset_wide ADD COLUMN a Array(UInt64);
SELECT 'dropped: wide control', k, a, length(a) FROM t_reoffset_wide ORDER BY k;

-- why: genuine sibling sharing, the case the id-space match must keep: `n.y` is added beside a live
-- `n.x`, shares the group's one offsets stream, and reads its DEFAULT filled to that length.
CREATE TABLE t_reoffset_sibling (k UInt64, `n.x` Array(UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 10000000, serialization_info_version = 'with_column_ids';
INSERT INTO t_reoffset_sibling VALUES (1, [10, 20, 30]);
ALTER TABLE t_reoffset_sibling ADD COLUMN `n.y` Array(String);
SELECT 'sibling: shares the group length', k, `n.x`, `n.y`, length(`n.y`) FROM t_reoffset_sibling ORDER BY k;

-- why: after a cross-parent rename the part still carries the pre-rename logical prefix (`n.x`) while
-- the schema says `m.x`, so only the id prefix still identifies the group. `m.y` shares it.
CREATE TABLE t_reoffset_moved (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 10000000, serialization_info_version = 'with_column_ids';
ALTER TABLE t_reoffset_moved ADD COLUMN `n.x` Array(UInt64);
INSERT INTO t_reoffset_moved VALUES (1, [10, 20, 30]);
ALTER TABLE t_reoffset_moved RENAME COLUMN `n.x` TO `m.x`;
ALTER TABLE t_reoffset_moved ADD COLUMN `m.y` Array(String);
SELECT 'moved: id prefix still groups them', k, `m.x`, `m.y`, length(`m.y`) FROM t_reoffset_moved ORDER BY k;
-- The part holds only `m.x`; `m.y` postdates it. Its id names the group the read above resolved.
SELECT 'moved: the column the part holds, and its id', column, column_id FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_reoffset_moved' AND active AND column LIKE 'm.%' ORDER BY column;

DROP TABLE t_reoffset_dropped SYNC;
DROP TABLE t_reoffset_wide SYNC;
DROP TABLE t_reoffset_sibling SYNC;
DROP TABLE t_reoffset_moved SYNC;
