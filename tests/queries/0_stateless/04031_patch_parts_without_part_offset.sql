-- Tags: no-parallel-replicas, no-replicated-database
-- Regression test: reading patch parts must work even when _part_offset
-- column is not explicitly requested. Previously, min/max part offset
-- was only computed when _part_offset was in the read sample block,
-- causing a LOGICAL_ERROR in MergeTreePatchReaderMerge.

SET insert_keeper_fault_injection_probability = 0.0;
SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS t_patch_offset SYNC;

CREATE TABLE t_patch_offset (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_patch_offset/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

INSERT INTO t_patch_offset VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Create a patch part via lightweight update.
UPDATE t_patch_offset SET value = 'x' WHERE id = 2;

-- Read without _part_offset — this must not throw.
SELECT * FROM t_patch_offset ORDER BY id;

-- Also test with PREWHERE to exercise another code path.
SELECT * FROM t_patch_offset PREWHERE value != '' ORDER BY id;

-- Also test selecting only a subset of columns.
SELECT id FROM t_patch_offset ORDER BY id;
SELECT value FROM t_patch_offset ORDER BY id;

DROP TABLE t_patch_offset SYNC;
