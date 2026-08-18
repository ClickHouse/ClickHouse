-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database
-- no-shared-merge-tree: relies on the merge assignment of ReplicatedMergeTree
-- no-replicated-database: fails due to additional shard

-- A merge of patch parts across the data version of an existing part used to produce a patch that
-- neither wholly applies nor wholly does not apply to that part, so every later operation on the
-- partition failed with "Found patch part ... that intersects mutation with version ...".
-- Related: https://github.com/ClickHouse/ClickHouse/issues/98898

DROP TABLE IF EXISTS t_lwu_span SYNC;

CREATE TABLE t_lwu_span (id UInt64, c1 UInt64, c2 UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_span/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    -- Keep the patch that the mutation below applies, so that it is still there when the newer
    -- patches are merged. In production the same window is open between the moment a mutation
    -- finishes and the moment the patch it applied is cleaned up.
    remove_unused_patch_parts = 0,
    -- Only the explicit OPTIMIZE FINAL below may merge the patch parts.
    max_bytes_to_merge_at_max_space_in_pool = 1;

SET insert_keeper_fault_injection_probability = 0;
INSERT INTO t_lwu_span SELECT number, number, number FROM numbers(10);

SET enable_lightweight_update = 1;
UPDATE t_lwu_span SET c1 = 100 WHERE id = 1;

-- A regular mutation gives the part a data version above the patch above and applies that patch.
ALTER TABLE t_lwu_span UPDATE c2 = 200 WHERE id = 2 SETTINGS mutations_sync = 2;

-- The merge predicate takes the boundary between patches from the mutations it still knows about.
-- A finished mutation is eventually removed from there while the data version it gave to the parts
-- stays; `KILL MUTATION` reaches that state at once.
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_span' SYNC FORMAT Null;

UPDATE t_lwu_span SET c1 = 300 WHERE id = 3;
UPDATE t_lwu_span SET c1 = 400 WHERE id = 4;

OPTIMIZE TABLE t_lwu_span FINAL;

-- The patch that the mutation has already applied must not be merged with the newer ones, so more
-- than one patch part is left. A merge of all of them spans the data version of the part, and the
-- resulting patch neither wholly applies nor wholly does not apply to it.
SELECT count() > 1 FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_span' AND active AND startsWith(name, 'patch');

ALTER TABLE t_lwu_span DETACH PARTITION ID 'all';

SELECT id, c1, c2 FROM t_lwu_span ORDER BY id;

DROP TABLE t_lwu_span SYNC;
