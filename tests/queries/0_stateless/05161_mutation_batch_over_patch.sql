-- Several mutations of one part can be executed as a single task. Patch parts are applied to the
-- source part before any of the commands of that task, so the batch must not span the version of a
-- patch part: a command created before the patch would be evaluated over an update it must not see.

DROP TABLE IF EXISTS t_mutation_batch_over_patch;

CREATE TABLE t_mutation_batch_over_patch (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_mutation_batch_over_patch VALUES (1, 1), (5, 2);

SYSTEM STOP MERGES t_mutation_batch_over_patch;
SET mutations_sync = 0;

ALTER TABLE t_mutation_batch_over_patch UPDATE c = c + 100 WHERE 1;

UPDATE t_mutation_batch_over_patch SET c = 0 WHERE 1;

-- Created while the mutation above is still pending, so that both are picked into a single task.
ALTER TABLE t_mutation_batch_over_patch DELETE WHERE 0;

SELECT 'update, on the fly';
SELECT id, c FROM t_mutation_batch_over_patch ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_mutation_batch_over_patch;
SET mutations_sync = 2;
ALTER TABLE t_mutation_batch_over_patch DELETE WHERE 0;

SELECT 'update, materialized';
SELECT id, c FROM t_mutation_batch_over_patch ORDER BY id;

DROP TABLE t_mutation_batch_over_patch;

-- The same for a lightweight `DELETE`, which becomes a mutation of the form
-- `UPDATE _row_exists = 0 WHERE pred`.

CREATE TABLE t_mutation_batch_over_patch (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_mutation_batch_over_patch VALUES (1, 1), (5, 2);

SYSTEM STOP MERGES t_mutation_batch_over_patch;
SET lightweight_deletes_sync = 0, mutations_sync = 0;

DELETE FROM t_mutation_batch_over_patch WHERE c = 1;

UPDATE t_mutation_batch_over_patch SET c = 1 WHERE id = 5;

ALTER TABLE t_mutation_batch_over_patch DELETE WHERE 0;

SELECT 'delete, on the fly';
SELECT id, c FROM t_mutation_batch_over_patch ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_mutation_batch_over_patch;
SET mutations_sync = 2;
ALTER TABLE t_mutation_batch_over_patch DELETE WHERE 0;

SELECT 'delete, materialized';
SELECT id, c FROM t_mutation_batch_over_patch ORDER BY id;

DROP TABLE t_mutation_batch_over_patch;
