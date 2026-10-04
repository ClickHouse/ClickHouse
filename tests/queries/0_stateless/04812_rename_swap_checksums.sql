-- Tags: replica, no-shared-merge-tree, no-replicated-database
-- no-shared-merge-tree -- the on-fly alter snapshot differs; ReplicatedMergeTree is required here
-- no-replicated-database -- profile events for mutations may differ because of additional replicas

-- A part whose metadata_version is two rename generations behind the table collapses both
-- generations into the swap {a.bin -> b.bin, b.bin -> a.bin}. DETACH PART spanning both renames
-- plus ATTACH produces that, because ATTACH preserves the part's own metadata_version.
DROP TABLE IF EXISTS t_rename_swap SYNC;

CREATE TABLE t_rename_swap (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rename_swap', '1') ORDER BY tuple()
-- The remap being tested lives in the partial-rewrite mutation task, which requires a wide part in
-- full (non-packed) storage. Both settings are randomized by the test runner, and a DDL setting wins
-- over the runner's injection, so pinning them here keeps the mutation on that path.
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- The fixture names parts literally, so the insert must not be able to abandon its block
-- number and land as all_1_1_0: a keeper fault injection retry allocates a fresh one.
INSERT INTO t_rename_swap SETTINGS insert_keeper_fault_injection_probability = 0 VALUES (111, 222, 333);

ALTER TABLE t_rename_swap DETACH PART 'all_0_0_0';
ALTER TABLE t_rename_swap RENAME COLUMN a TO a1, RENAME COLUMN b TO b1 SETTINGS alter_sync = 2;
ALTER TABLE t_rename_swap RENAME COLUMN a1 TO b, RENAME COLUMN b1 TO a SETTINGS alter_sync = 2;
ALTER TABLE t_rename_swap ATTACH PART 'all_0_0_0' SETTINGS insert_keeper_fault_injection_probability = 0;

-- Materializes the swap. `c` is deliberately outside the rename map.
ALTER TABLE t_rename_swap UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 2;

-- The remap under test runs only in the partial-rewrite mutation task. Assert the task itself: both
-- tasks inherit the same part format, so part_type and part_storage_type cannot distinguish them.
SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MutationSomePartColumns'], ProfileEvents['MutationAllPartColumns']
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND database = currentDatabase() AND table = 't_rename_swap' AND event_type = 'MutatePart'
ORDER BY ALL;

-- Reading the swapped columns: a missing checksum entry makes this throw LOGICAL_ERROR.
SELECT a, b, c FROM t_rename_swap ORDER BY c;

-- The part must stay intact. A part missing a checksum entry for a column that columns.txt still
-- lists is quarantined on the next consistency-checking load, taking the part it covers with it.
-- These rows stay observable in a release build, where the read above does not abort.
SELECT count() FROM system.parts WHERE table = 't_rename_swap' AND database = currentDatabase() AND active;
SELECT count() FROM system.detached_parts WHERE table = 't_rename_swap' AND database = currentDatabase();
CHECK TABLE t_rename_swap SETTINGS check_query_single_value_result = 1;

DROP TABLE t_rename_swap SYNC;
