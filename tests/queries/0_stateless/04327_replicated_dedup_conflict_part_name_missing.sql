-- Tags: no-parallel, no-shared-merge-tree
-- no-parallel: uses a global failpoint in ReplicatedMergeTreeSink::commitPart.
-- no-shared-merge-tree: the failpoint is specific to ReplicatedMergeTreeSink.

-- Regression test for a logical error 'conflicted_part_name.has_value()' in
-- ReplicatedMergeTreeSink::finishDelayed. When a synchronous insert is fully
-- deduplicated, the sink logged the name of the conflicting part. That name is
-- resolved from the dedup node in Keeper, but the node can legitimately be gone
-- by then (e.g. concurrent DROP PARTITION removed it), in which case the part
-- name stays unset and reading it tripped a chassert. The failpoint below forces
-- that "name not resolved" state deterministically.

SYSTEM DISABLE FAILPOINT rmt_dedup_conflict_part_name_missing;

DROP TABLE IF EXISTS t_04327 SYNC;

CREATE TABLE t_04327 (k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04327_dedup/t', 'r1')
ORDER BY k;

-- First insert commits the block and registers its deduplication hash in Keeper.
INSERT INTO t_04327 SETTINGS async_insert = 0 VALUES (1, 1), (2, 2), (3, 3);

SYSTEM ENABLE FAILPOINT rmt_dedup_conflict_part_name_missing;

-- Re-inserting the same data is fully deduplicated; the failpoint makes the
-- conflicting part name unresolved on the dedup-conflict path. Before the fix
-- this crashed the server; now it must dedup cleanly.
INSERT INTO t_04327 SETTINGS async_insert = 0 VALUES (1, 1), (2, 2), (3, 3);

SYSTEM DISABLE FAILPOINT rmt_dedup_conflict_part_name_missing;

-- The duplicate insert was ignored: still exactly 3 rows, server is alive.
SELECT count() FROM t_04327;

DROP TABLE t_04327 SYNC;
