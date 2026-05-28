-- Regression for the `backups_in_memory` slot of `Context::resetToUserDefaults`.
-- `BACKUP ... TO Memory(name)` stores the backup payload on the session
-- context's `BackupsInMemoryHolder`. If `RESET SESSION` did not clear it, a
-- pooled connection could leak backup state across reuse: the next borrower
-- could still `RESTORE FROM Memory(name)` or hit `BACKUP_ALREADY_EXISTS` when
-- trying to reuse the name — i.e. the session would not be back to its
-- post-authentication baseline.

DROP TABLE IF EXISTS reset_session_backup_src;

CREATE TABLE reset_session_backup_src (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO reset_session_backup_src VALUES (1), (2), (3);

BACKUP TABLE reset_session_backup_src TO Memory('reset_session_b1') FORMAT Null;

-- Sanity: restoring from the in-memory backup works pre-reset.
DROP TABLE reset_session_backup_src SYNC;
RESTORE TABLE reset_session_backup_src FROM Memory('reset_session_b1') FORMAT Null;
SELECT count() FROM reset_session_backup_src;

RESET SESSION;

-- After the reset the in-memory holder must be empty: the named backup is
-- gone, so restoring from it fails with `BACKUP_NOT_FOUND`.
RESTORE TABLE reset_session_backup_src FROM Memory('reset_session_b1') FORMAT Null; -- { serverError BACKUP_NOT_FOUND }

-- And the name is reusable: a fresh `BACKUP ... TO Memory('reset_session_b1')`
-- does not collide with the pre-reset entry.
BACKUP TABLE reset_session_backup_src TO Memory('reset_session_b1') FORMAT Null;
SELECT 'second backup succeeded';

DROP TABLE reset_session_backup_src;
