-- Tags: no-replicated-database
-- no-replicated-database: fails due to additional shard.

-- Plain (non-replicated) MergeTree REPLACE/MOVE/DETACH PARTITION and DETACH PART must reject a
-- source that has unapplied patch parts (from a not-yet-materialized lightweight UPDATE), exactly
-- like ReplicatedMergeTree already does (see 03100_lwu_22_detach_attach_patches.sql). These ops
-- clone/keep only the base parts, so the pending patch is orphaned and the update silently reverts
-- (DETACH then ATTACH would bring the base part back at the pre-update value). DROP stays allowed.

DROP TABLE IF EXISTS t_lwu_51_src;
DROP TABLE IF EXISTS t_lwu_51_dst;

SET enable_lightweight_update = 1;
SET mutations_sync = 2;

CREATE TABLE t_lwu_51_src (c0 Int)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_lwu_51_dst (c0 Int)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

-- Pin the part layout (two separate parts, an unapplied patch part) so the patch is not
-- merged away and the DETACH PART name below stays deterministic under randomized settings.
SYSTEM STOP MERGES t_lwu_51_src;
SYSTEM STOP MERGES t_lwu_51_dst;

INSERT INTO t_lwu_51_src VALUES (1);
INSERT INTO t_lwu_51_src VALUES (5);
UPDATE t_lwu_51_src SET c0 = c0 + 10 WHERE TRUE;

SELECT 'src', arraySort(groupArray(c0)) FROM t_lwu_51_src;

-- REPLACE PARTITION FROM self (logical no-op) must not silently drop the pending update.
ALTER TABLE t_lwu_51_src REPLACE PARTITION ID 'all' FROM t_lwu_51_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after rejected REPLACE self', arraySort(groupArray(c0)) FROM t_lwu_51_src;

-- REPLACE PARTITION from a different source that has pending patches.
INSERT INTO t_lwu_51_dst VALUES (100);
ALTER TABLE t_lwu_51_dst REPLACE PARTITION ID 'all' FROM t_lwu_51_src; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after rejected REPLACE other', arraySort(groupArray(c0)) FROM t_lwu_51_dst;

-- MOVE PARTITION from a source that has pending patches.
ALTER TABLE t_lwu_51_src MOVE PARTITION ID 'all' TO TABLE t_lwu_51_dst; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after rejected MOVE src', arraySort(groupArray(c0)) FROM t_lwu_51_src;
SELECT 'after rejected MOVE dst', arraySort(groupArray(c0)) FROM t_lwu_51_dst;

-- DETACH PARTITION on a partition with pending patches (re-attach would revert the update).
ALTER TABLE t_lwu_51_src DETACH PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after rejected DETACH PARTITION', arraySort(groupArray(c0)) FROM t_lwu_51_src;

-- DETACH PART on a part with pending patches.
ALTER TABLE t_lwu_51_src DETACH PART 'all_1_1_0'; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'after rejected DETACH PART', arraySort(groupArray(c0)) FROM t_lwu_51_src;

-- After APPLY PATCHES the patched data must travel with the partition.
-- (Re-enable merges first: APPLY PATCHES materializes the patch via a mutation.)
SYSTEM START MERGES t_lwu_51_src;
ALTER TABLE t_lwu_51_src APPLY PATCHES IN PARTITION ID 'all';
ALTER TABLE t_lwu_51_dst REPLACE PARTITION ID 'all' FROM t_lwu_51_src;
SELECT 'after APPLY PATCHES + REPLACE', arraySort(groupArray(c0)) FROM t_lwu_51_dst;

DROP TABLE t_lwu_51_src;
DROP TABLE t_lwu_51_dst;
