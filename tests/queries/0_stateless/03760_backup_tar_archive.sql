-- Tags: no-parallel, no-fasttest, no-flaky-check, no-encrypted-storage
-- Because we are creating a backup with fixed path.

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
CREATE TABLE t0 (c1 Map(Int,Int), c2 Int) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY (c1, c2 % 6084);
INSERT INTO TABLE t0 (c1, c2) SELECT map(), 2 FROM numbers(100);
SET min_insert_block_size_rows = 64, optimize_trivial_insert_select = 1;
INSERT INTO TABLE t0 (c1, c2) SELECT c1, c2 FROM generateRandom('c1 Map(Int,Int), c2 Int', 2607967781039168224, 1, 1) LIMIT 400;

BACKUP TABLE t0 TO Disk('default', '03760_backup_tar_archive') FORMAT Null;

RESTORE TABLE t0 AS t1 FROM Disk('default', '03760_backup_tar_archive') FORMAT Null;

SELECT * FROM t1 LIMIT 10;
