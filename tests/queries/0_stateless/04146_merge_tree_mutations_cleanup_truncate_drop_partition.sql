-- Test: exercises clearOldMutations(truncate=true) and the removal of clearOldMutations from dropPartition.
-- Covers:
--   src/Storages/StorageMergeTree.cpp:1840 — `if (!truncate && !finished_mutations_to_keep) return 0;` early-return MUST be bypassed when called via TRUNCATE.
--   src/Storages/StorageMergeTree.cpp:2391 — dropPartition no longer calls clearOldMutations, so finished mutations must persist after DROP PARTITION.

DROP TABLE IF EXISTS data_truncate_60031;
DROP TABLE IF EXISTS data_drop_part_60031;

-- Part 1: TRUNCATE TABLE must clear finished mutations even when finished_mutations_to_keep=0.
-- (clearOldMutations(truncate=true) bypasses the new early-return at line 1840 and forces finished_mutations_to_keep=0.)
CREATE TABLE data_truncate_60031 (key Int) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS finished_mutations_to_keep = 0;
INSERT INTO data_truncate_60031 VALUES (1);
ALTER TABLE data_truncate_60031 DELETE WHERE 1 SETTINGS mutations_sync = 1;
SELECT 'mutations before TRUNCATE', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'data_truncate_60031';
TRUNCATE TABLE data_truncate_60031;
SELECT 'mutations after TRUNCATE',  count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'data_truncate_60031';
DROP TABLE data_truncate_60031;

-- Part 2: DROP PARTITION must NOT clear finished mutations (the PR removed the clearOldMutations(true) call from dropPartition).
CREATE TABLE data_drop_part_60031 (key Int) ENGINE = MergeTree PARTITION BY key ORDER BY tuple()
    SETTINGS finished_mutations_to_keep = 100;
INSERT INTO data_drop_part_60031 VALUES (1);
INSERT INTO data_drop_part_60031 VALUES (2);
ALTER TABLE data_drop_part_60031 DELETE WHERE key = 999 SETTINGS mutations_sync = 1;
SELECT 'mutations before DROP PARTITION', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'data_drop_part_60031';
ALTER TABLE data_drop_part_60031 DROP PARTITION 1;
SELECT 'mutations after DROP PARTITION',  count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'data_drop_part_60031';
DROP TABLE data_drop_part_60031;
