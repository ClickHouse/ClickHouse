-- A pending lightweight `DELETE` evaluates its predicate over the data as of its own mutation
-- version, so a patch part created after it is not visible to that predicate - the on-fly read must
-- agree with what materializing the mutation produces.

DROP TABLE IF EXISTS t_pending_delete_patch;

CREATE TABLE t_pending_delete_patch (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_pending_delete_patch VALUES (1, 1), (5, 2);

SYSTEM STOP MERGES t_pending_delete_patch;

SET lightweight_deletes_sync = 0, mutations_sync = 0;
DELETE FROM t_pending_delete_patch WHERE c = 1;

UPDATE t_pending_delete_patch SET c = 1 WHERE id = 5;

SELECT 'on the fly';
SELECT id, c FROM t_pending_delete_patch ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT 'without the patch';
SELECT id, c FROM t_pending_delete_patch ORDER BY id SETTINGS apply_mutations_on_fly = 1, apply_patch_parts = 0;

SYSTEM START MERGES t_pending_delete_patch;
SET mutations_sync = 2;
ALTER TABLE t_pending_delete_patch DELETE WHERE 0;

SELECT 'materialized';
SELECT id, c FROM t_pending_delete_patch ORDER BY id;

DROP TABLE t_pending_delete_patch;

-- The reverse order: the patch is older than the `DELETE`, so its value is what the predicate sees.

CREATE TABLE t_pending_delete_patch (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_pending_delete_patch VALUES (1, 1), (5, 2);

UPDATE t_pending_delete_patch SET c = 1 WHERE id = 5;

SYSTEM STOP MERGES t_pending_delete_patch;
SET lightweight_deletes_sync = 0, mutations_sync = 0;
DELETE FROM t_pending_delete_patch WHERE c = 1;

SELECT 'patch first, on the fly';
SELECT count() FROM t_pending_delete_patch SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_pending_delete_patch;
SET mutations_sync = 2;
ALTER TABLE t_pending_delete_patch DELETE WHERE 0;

SELECT 'patch first, materialized';
SELECT count() FROM t_pending_delete_patch;

DROP TABLE t_pending_delete_patch;
