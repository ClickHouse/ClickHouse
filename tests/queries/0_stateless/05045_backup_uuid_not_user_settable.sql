-- `backup_uuid` is internal: a backup proves the destination's lock file is its own by comparing it
-- against this UUID, so a user-chosen value could repeat the UUID of a backup in progress and take
-- over its lock. An explicit `backup_uuid` must be rejected, like the `internal` setting.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1);

BACKUP TABLE t TO Disk('backups', currentDatabase() || '_05045') SETTINGS backup_uuid = '12345678-1234-1234-1234-123456789abc' FORMAT Null; -- { serverError ACCESS_DENIED }

DROP TABLE t;
