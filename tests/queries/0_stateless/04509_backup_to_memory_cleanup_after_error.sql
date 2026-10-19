-- Tags: no-parallel
-- no-parallel: enables a global failpoint

-- Regression test: a BACKUP ... TO Memory(...) that fails before finalization (before the `.backup`
-- metadata file is written) must still clean up after itself. The cleanup used to throw
-- BACKUP_ENTRY_NOT_FOUND on the missing `.backup` file and abort, leaving the backup registered, so
-- re-using the same name failed with BACKUP_ALREADY_EXISTS.

DROP TABLE IF EXISTS t_backup_mem_cleanup;

CREATE TABLE t_backup_mem_cleanup (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_backup_mem_cleanup VALUES (1), (2), (3);

SYSTEM ENABLE FAILPOINT backup_fail_before_writing_metadata;

-- The backup fails before writing metadata; its cleanup must remove the partially written backup.
BACKUP TABLE t_backup_mem_cleanup TO Memory('mem_cleanup') FORMAT Null; -- { serverError FAULT_INJECTED }

SYSTEM DISABLE FAILPOINT backup_fail_before_writing_metadata;

-- The name must be free again: re-using it must succeed (it would fail with BACKUP_ALREADY_EXISTS
-- if the failed backup had been left behind).
BACKUP TABLE t_backup_mem_cleanup TO Memory('mem_cleanup') FORMAT Null;

DROP TABLE t_backup_mem_cleanup;
