-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation this test needs to stay pending
-- The per-level MATERIALIZED recompute stages belong to the pending UPDATE, so they have to carry
-- its mutation version: on-fly reads derive the patch-visibility window from consecutive stages'
-- versions, and without a version the patch becomes visible to the level stages, which then
-- recompute `m1` from the patched `z` and override the value the patch carries itself.

SET alter_sync = 0, mutations_sync = 0;

DROP TABLE IF EXISTS t_on_fly_patch;

CREATE TABLE t_on_fly_patch
(
    id UInt64,
    x Int32,
    z Int32,
    m1 Int32 MATERIALIZED x + z,
    m2 Int32 MATERIALIZED m1 + 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_on_fly_patch (id, x, z) VALUES (1, 10, 100);

SYSTEM STOP MERGES t_on_fly_patch;

SET enable_lightweight_update = 1, apply_patch_parts = 1;
ALTER TABLE t_on_fly_patch UPDATE x = 20 WHERE 1;

-- The lightweight update must not read through the pending mutation, otherwise the `m1` it stores
-- in the patch is already computed from `x = 20` and coincides with what recomputing it would give.
SET apply_mutations_on_fly = 0;
UPDATE t_on_fly_patch SET z = 500 WHERE 1;
SET apply_mutations_on_fly = 1;

-- The on-fly read must agree with reading the materialized part plus its patch: `m1` in the patch
-- was computed from `x = 10`, the value visible when the lightweight update ran.
SELECT x, z, m1, m2 FROM t_on_fly_patch;
SELECT m1 FROM t_on_fly_patch;
SELECT m2 FROM t_on_fly_patch;

SYSTEM START MERGES t_on_fly_patch;

DROP TABLE t_on_fly_patch;
