-- Tags: zookeeper

-- A lightweight UPDATE that fails while its pipeline runs must release the lightweight update lock.
-- The follow-up updates use lock_acquire_timeout = 0, so they fail at once if the lock is still held.

DROP TABLE IF EXISTS t_lwu_lock_release;

CREATE TABLE t_lwu_lock_release (id UInt64, s String, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_lock_release VALUES (1, 'aa', 0) (2, 'bb', 0) (3, 'cc', 0);

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

UPDATE t_lwu_lock_release SET s = 'xx' WHERE id = 1 AND throwIf(id = 1) SETTINGS update_parallel_mode = 'sync'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
UPDATE t_lwu_lock_release SET s = 'yy' WHERE id = 2 SETTINGS update_parallel_mode = 'sync', lock_acquire_timeout = 0;

-- In auto mode the follow-up has to read the column the failed update wrote, otherwise they do not conflict.
UPDATE t_lwu_lock_release SET s = 'zz' WHERE id = 3 AND throwIf(id = 3) SETTINGS update_parallel_mode = 'auto'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
UPDATE t_lwu_lock_release SET v = 7 WHERE s = 'yy' SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 0;

SELECT id, s, v FROM t_lwu_lock_release ORDER BY id;

DROP TABLE t_lwu_lock_release;

-- The replicated table holds the same lock in Keeper, released on a different path.
DROP TABLE IF EXISTS t_lwu_lock_release_rep SYNC;

CREATE TABLE t_lwu_lock_release_rep (id UInt64, s String, v UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_lock_release_rep/', '1') ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_lock_release_rep VALUES (1, 'aa', 0) (2, 'bb', 0) (3, 'cc', 0);

UPDATE t_lwu_lock_release_rep SET s = 'xx' WHERE id = 1 AND throwIf(id = 1) SETTINGS update_parallel_mode = 'sync'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
UPDATE t_lwu_lock_release_rep SET s = 'yy' WHERE id = 2 SETTINGS update_parallel_mode = 'sync', lock_acquire_timeout = 0;

UPDATE t_lwu_lock_release_rep SET s = 'zz' WHERE id = 3 AND throwIf(id = 3) SETTINGS update_parallel_mode = 'auto'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
UPDATE t_lwu_lock_release_rep SET v = 7 WHERE s = 'yy' SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 0;

SELECT id, s, v FROM t_lwu_lock_release_rep ORDER BY id;

DROP TABLE t_lwu_lock_release_rep SYNC;
