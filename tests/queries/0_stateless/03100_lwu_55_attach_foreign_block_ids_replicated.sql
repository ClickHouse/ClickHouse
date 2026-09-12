-- Tags: no-replicated-database
-- no-replicated-database: fails due to additional shard.

-- ReplicatedMergeTree half of 03100_lwu_54_attach_foreign_block_ids: the same cross-table adoption
-- rejections, plus FETCH PARTITION. Split off so neither file crosses the flaky check's 180s cap,
-- which the per-statement ThreadFuzzer overhead makes reachable for a long single file.

SET enable_lightweight_update = 1;
SET mutations_sync = 2;

-- SYNC so a re-run in the same database does not hit a leftover replica in ZooKeeper, as
-- 03100_lwu_22_detach_attach_patches.sql does.
DROP TABLE IF EXISTS t_lwu_55_rmt_dst, t_lwu_55_rmt_src, t_lwu_55_rmt_src_nocol,
    t_lwu_55_rmt_mixed, t_lwu_55_rmt_fetch SYNC;

CREATE TABLE t_lwu_55_rmt_dst (p UInt8, x UInt64, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_55_rmt_dst', '1') PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, min_bytes_for_wide_part = 0;

CREATE TABLE t_lwu_55_rmt_src (p UInt8, x UInt64, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_55_rmt_src', '1') PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_lwu_55_rmt_src SELECT 1, number + 100, 1000 FROM numbers(5);
INSERT INTO t_lwu_55_rmt_src SELECT 1, number + 200, 1000 FROM numbers(5);
OPTIMIZE TABLE t_lwu_55_rmt_src PARTITION 1 FINAL;

INSERT INTO t_lwu_55_rmt_dst SELECT 1, number, 0 FROM numbers(10);
UPDATE t_lwu_55_rmt_dst SET y = 5 WHERE p = 1;

ALTER TABLE t_lwu_55_rmt_dst ATTACH PARTITION ID '1' FROM t_lwu_55_rmt_src;   -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_lwu_55_rmt_dst REPLACE PARTITION ID '1' FROM t_lwu_55_rmt_src;  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_lwu_55_rmt_src MOVE PARTITION ID '1' TO TABLE t_lwu_55_rmt_dst; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'rmt dst after rejected adoptions', arraySort(groupArray((x, y))) FROM t_lwu_55_rmt_dst;

-- Allowed: self-REPLACE on ReplicatedMergeTree. Its exemption condition is separate from the plain
-- MergeTree one (&src_data == this in replacePartitionFromImpl, which has no source_table in scope).
ALTER TABLE t_lwu_55_rmt_src REPLACE PARTITION ID '1' FROM t_lwu_55_rmt_src;
SELECT 'rmt src after allowed self-REPLACE', arraySort(groupArray((x, y))) FROM t_lwu_55_rmt_src;

-- FETCH PARTITION of a merged, identity-bearing part is rejected. The FETCH is same-path (from the
-- table's own zk path) so it takes the local-clone path and is deterministic in any environment; it
-- exercises the exact same to_detached chokepoint in fetchPart that a cross-table FETCH does. A
-- same-path FETCH is itself a real adoption door: it clones the active part into detached/ without
-- removing the original, so a later ATTACH PART would reproduce the self-ATTACH duplicate-identity
-- defect.
ALTER TABLE t_lwu_55_rmt_src FETCH PARTITION ID '1' FROM '/clickhouse/tables/{database}/t_lwu_55_rmt_src'; -- { serverError SUPPORT_IS_DISABLED }
-- The rejection happens before the part is preserved and renamed into detached/, so its destructor
-- must have removed the temp directory and nothing is left to attach.
SELECT 'rmt src has no detached parts after rejected fetch', count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_lwu_55_rmt_src';

-- Mixed partition: one merged identity-bearing part plus a later level-0 part without the identity
-- columns. fetchPartition fetches parts concurrently and rethrows only after all of them finish, so
-- the level-0 part is renamed into detached/ before the merged one is rejected. The rejected
-- statement must still leave nothing behind, otherwise it is not retryable: the retry would fail on
-- checkIfDetachedPartitionExists with PARTITION_ALREADY_EXISTS, and meanwhile the leftover part is
-- attachable.
CREATE TABLE t_lwu_55_rmt_mixed (p UInt8, x UInt64, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_55_rmt_mixed', '1') PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;
-- Partition 2 is detached up front: the cleanup must remove only what the failed FETCH itself put
-- in detached/, never a part that was already there.
INSERT INTO t_lwu_55_rmt_mixed SELECT 2, number + 900, 9 FROM numbers(5);
ALTER TABLE t_lwu_55_rmt_mixed DETACH PARTITION 2;
INSERT INTO t_lwu_55_rmt_mixed SELECT 1, number + 400, 8 FROM numbers(5);
INSERT INTO t_lwu_55_rmt_mixed SELECT 1, number + 500, 8 FROM numbers(5);
OPTIMIZE TABLE t_lwu_55_rmt_mixed PARTITION 1 FINAL;
SYSTEM STOP MERGES t_lwu_55_rmt_mixed;
INSERT INTO t_lwu_55_rmt_mixed SELECT 1, number + 600, 8 FROM numbers(5);
SELECT 'rmt mixed partition has an identity-bearing part and a level-0 part',
    countIf(c > 0), countIf(c = 0)
FROM
(
    SELECT name, countIf(column IN ('_block_number', '_block_offset')) AS c
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_lwu_55_rmt_mixed' AND active
    GROUP BY name
);
ALTER TABLE t_lwu_55_rmt_mixed FETCH PARTITION ID '1' FROM '/clickhouse/tables/{database}/t_lwu_55_rmt_mixed'; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'rmt mixed detached parts after rejected fetch (only the pre-existing one)',
    count(), countIf(partition_id = '2')
FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_lwu_55_rmt_mixed';
-- Retryable: the same statement fails again with the same rejection, not PARTITION_ALREADY_EXISTS.
ALTER TABLE t_lwu_55_rmt_mixed FETCH PARTITION ID '1' FROM '/clickhouse/tables/{database}/t_lwu_55_rmt_mixed'; -- { serverError SUPPORT_IS_DISABLED }
SYSTEM START MERGES t_lwu_55_rmt_mixed;
DROP TABLE t_lwu_55_rmt_mixed SYNC;

-- Allowed: FETCH of a column-less part (level-0, no persisted identity columns).
CREATE TABLE t_lwu_55_rmt_src_nocol (p UInt8, x UInt64, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_55_rmt_src_nocol', '1') PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_lwu_55_rmt_src_nocol SELECT 1, number + 300, 7 FROM numbers(5);
ALTER TABLE t_lwu_55_rmt_src_nocol FETCH PARTITION ID '1' FROM '/clickhouse/tables/{database}/t_lwu_55_rmt_src_nocol';
SELECT 'rmt fetch of column-less part succeeded', count() > 0 FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_lwu_55_rmt_src_nocol';

DROP TABLE IF EXISTS t_lwu_55_rmt_dst, t_lwu_55_rmt_src, t_lwu_55_rmt_src_nocol SYNC;
