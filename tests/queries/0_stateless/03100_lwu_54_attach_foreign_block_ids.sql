-- Tags: no-replicated-database
-- no-replicated-database: fails due to additional shard.

-- Cross-table partition adoption (REPLACE/ATTACH/MOVE PARTITION FROM, CLONE AS, cross-table FETCH)
-- clones parts by hardlink and keeps their content unchanged. When the source table has
-- enable_block_number_column / enable_block_offset_column enabled, the _block_number / _block_offset
-- columns are materialized at merge time and physically stored in the part, so adopting a merged
-- source part imports the source table's block identities. Every table numbers blocks from 1, so the
-- imported (_block_number, _block_offset) pairs collide with the destination's own. A later merge
-- gathers the duplicates into a single part; applying a lightweight-update patch in Join mode then
-- aborts on the sorted/unique assertion in applyPatchJoin (debug/sanitizer) or silently patches the
-- wrong rows (release). The guard rejects the adoption with SUPPORT_IS_DISABLED.

SET enable_lightweight_update = 1;
SET mutations_sync = 2;

-- clone1 is created by a CLONE AS that the guard rejects after the table is already registered, so it
-- survives the error.
DROP TABLE IF EXISTS t_lwu_54_dst, t_lwu_54_src, t_lwu_54_src_nocol, t_lwu_54_src_offset_only,
    t_lwu_54_src_number_only, t_lwu_54_clone1, t_lwu_54_clone2;

CREATE TABLE t_lwu_54_dst (p UInt8, x UInt64, y UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, min_bytes_for_wide_part = 0;

CREATE TABLE t_lwu_54_src (p UInt8, x UInt64, y UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;

-- The source part must be MERGED: level-0 inserts do not persist the identity columns, only
-- level >= 1 parts carry them. Two inserts + OPTIMIZE FINAL produce a merged part with the columns.
INSERT INTO t_lwu_54_src SELECT 1, number + 100, 1000 FROM numbers(5);
INSERT INTO t_lwu_54_src SELECT 1, number + 200, 1000 FROM numbers(5);
OPTIMIZE TABLE t_lwu_54_src PARTITION 1 FINAL;

INSERT INTO t_lwu_54_dst SELECT 1, number, 0 FROM numbers(10);
UPDATE t_lwu_54_dst SET y = 5 WHERE p = 1;

-- All three cross-table adoptions of the merged, identity-bearing source part are rejected.
ALTER TABLE t_lwu_54_dst ATTACH PARTITION ID '1' FROM t_lwu_54_src;   -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_lwu_54_dst REPLACE PARTITION ID '1' FROM t_lwu_54_src;  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_lwu_54_src MOVE PARTITION ID '1' TO TABLE t_lwu_54_dst; -- { serverError SUPPORT_IS_DISABLED }

-- The destination is untouched by the rejected operations.
SELECT 'dst after rejected adoptions', arraySort(groupArray((x, y))) FROM t_lwu_54_dst;

-- Allowed: a column-less source (no merge happened, so its single part is level-0 and carries no
-- persisted identity columns). REPLACE assigns fresh destination identities.
CREATE TABLE t_lwu_54_src_nocol (p UInt8, x UInt64, y UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_lwu_54_src_nocol SELECT 1, number + 300, 7 FROM numbers(5);
ALTER TABLE t_lwu_54_dst REPLACE PARTITION ID '1' FROM t_lwu_54_src_nocol;
SELECT 'dst after allowed REPLACE from column-less src', arraySort(groupArray((x, y))) FROM t_lwu_54_dst;

-- Allowed: self-REPLACE on a table with merged, identity-bearing parts and no pending patches. The
-- originals are removed, so identities stay native and unique.
ALTER TABLE t_lwu_54_src REPLACE PARTITION ID '1' FROM t_lwu_54_src;
SELECT 'src after allowed self-REPLACE', arraySort(groupArray((x, y))) FROM t_lwu_54_src;

-- CLONE AS lowers to ATTACH PARTITION ALL FROM: rejected for a merged columned source, allowed for
-- a column-less source.
CREATE TABLE t_lwu_54_clone1 CLONE AS t_lwu_54_src;        -- { serverError SUPPORT_IS_DISABLED }
-- CLONE AS registers the table before running the internal ATTACH and does not roll back, so the
-- rejection must leave the table with no adopted part.
SELECT 'clone1 has no adopted parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_54_clone1' AND active;
CREATE TABLE t_lwu_54_clone2 CLONE AS t_lwu_54_src_nocol;
SELECT 'clone2 from column-less src', arraySort(groupArray((x, y))) FROM t_lwu_54_clone2;

-- Correctness postlude: lightweight update on the destination (whose partition now came from the
-- allowed REPLACE) applies to exactly the intended rows, with no duplicate-identity corruption.
UPDATE t_lwu_54_dst SET y = 42 WHERE x = 302;
OPTIMIZE TABLE t_lwu_54_dst PARTITION 1 FINAL;
SELECT 'dst after correctness postlude', arraySort(groupArray((x, y))) FROM t_lwu_54_dst;

-- The two settings are independent, so a source can persist only one of the identity columns. Such a
-- table cannot do lightweight updates itself, but it can still be adopted into a destination that
-- can, which is why the check tests each column separately.
CREATE TABLE t_lwu_54_src_offset_only (p UInt8, x UInt64, y UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_lwu_54_src_offset_only SELECT 1, number + 400, 3 FROM numbers(5);
INSERT INTO t_lwu_54_src_offset_only SELECT 1, number + 500, 3 FROM numbers(5);
OPTIMIZE TABLE t_lwu_54_src_offset_only PARTITION 1 FINAL;

ALTER TABLE t_lwu_54_dst ATTACH PARTITION ID '1' FROM t_lwu_54_src_offset_only; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'dst after rejected offset-only adoption', arraySort(groupArray((x, y))) FROM t_lwu_54_dst;

-- The other single-column case: only _block_number is persisted.
CREATE TABLE t_lwu_54_src_number_only (p UInt8, x UInt64, y UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_lwu_54_src_number_only SELECT 1, number + 600, 4 FROM numbers(5);
INSERT INTO t_lwu_54_src_number_only SELECT 1, number + 700, 4 FROM numbers(5);
OPTIMIZE TABLE t_lwu_54_src_number_only PARTITION 1 FINAL;

ALTER TABLE t_lwu_54_dst ATTACH PARTITION ID '1' FROM t_lwu_54_src_number_only; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'dst after rejected number-only adoption', arraySort(groupArray((x, y))) FROM t_lwu_54_dst;

DROP TABLE IF EXISTS t_lwu_54_dst, t_lwu_54_src, t_lwu_54_src_nocol, t_lwu_54_src_offset_only,
    t_lwu_54_src_number_only, t_lwu_54_clone1, t_lwu_54_clone2;
