-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas: profile events may differ with parallel replicas.
-- no-replicated-database: fails due to additional shard.

-- v2 patch parts store the sort-key columns they were written with only to identify updated rows.
-- After ALTER MODIFY ORDER BY narrows the sorting key, such columns are no longer part of the
-- effective key, but they still must not be treated as updated by the patch.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_narrowed_key SYNC;

CREATE TABLE t_lwu_narrowed_key (a UInt64, b UInt64, v String)
ENGINE = MergeTree
PRIMARY KEY a
ORDER BY (a, b)
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0,
    patch_parts_version = 'v2';

INSERT INTO t_lwu_narrowed_key SELECT number, (number + 1) * 10, 'foo' FROM numbers(1000);

-- The patch is persisted under (a, b) and stores b only as row identity.
UPDATE t_lwu_narrowed_key SET v = 'bar' WHERE a < 100;

ALTER TABLE t_lwu_narrowed_key MODIFY ORDER BY a;

-- b is not updated by the patch, so reading it must not apply the patch.
SELECT sum(b) FROM t_lwu_narrowed_key;
SELECT count() FROM t_lwu_narrowed_key WHERE v = 'bar';

-- New patches are persisted under the narrowed key (a): b is updatable now.
-- Patches persisted under (a, b) and (a) have the same effective key.
UPDATE t_lwu_narrowed_key SET b = 0 WHERE a >= 900;
UPDATE t_lwu_narrowed_key SET v = 'baz' WHERE a < 50;

-- This time reading b must apply the patch that updates it (and only it).
SELECT sum(b) FROM t_lwu_narrowed_key;
SELECT count() FROM t_lwu_narrowed_key WHERE v = 'bar';
SELECT count() FROM t_lwu_narrowed_key WHERE v = 'baz';
SELECT count() FROM t_lwu_narrowed_key WHERE b = 0;

DROP TABLE t_lwu_narrowed_key SYNC;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReadTasksWithAppliedPatches'] > 0, ProfileEvents['PatchesAppliedInAllReadTasks'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND query LIKE '%SELECT sum(b) FROM t_lwu_narrowed_key%' AND query NOT LIKE '%query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds;
