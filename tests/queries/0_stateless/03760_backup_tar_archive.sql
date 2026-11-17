-- Tags: no-parallel, no-fasttest, no-flaky-check, no-encrypted-storage
-- Because we are creating a backup with fixed path.

DROP TABLE IF EXISTS t0 SYNC;
DROP TABLE IF EXISTS t1 SYNC;

CREATE TABLE t0 (c1 Int) ENGINE = MergeTree() ORDER BY c1 PARTITION BY (c1 % 6451);
SET min_insert_block_size_rows = 64, optimize_trivial_insert_select = 1;
INSERT INTO TABLE t0 (c1) SELECT number FROM numbers(500);

BACKUP TABLE t0 TO Disk('backups', '03760_backup_tar_archive.tar') FORMAT Null;

RESTORE TABLE t0 AS t1 FROM Disk('backups', '03760_backup_tar_archive.tar') FORMAT Null;

SELECT * FROM t1 ORDER BY c1 LIMIT 10;
