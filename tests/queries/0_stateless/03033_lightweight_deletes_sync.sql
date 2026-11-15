DROP TABLE IF EXISTS t_lightweight_deletes;

CREATE TABLE t_lightweight_deletes (a UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_lightweight_deletes VALUES (1) (2) (3);

DELETE FROM t_lightweight_deletes WHERE a = 1 SETTINGS lightweight_deletes_sync = 2;

SELECT count() FROM t_lightweight_deletes;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_deletes' AND NOT is_done;

SYSTEM STOP MERGES t_lightweight_deletes;
DELETE FROM t_lightweight_deletes WHERE a = 2 SETTINGS lightweight_deletes_sync = 0;

SELECT count() FROM t_lightweight_deletes;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lightweight_deletes' AND NOT is_done;

DROP TABLE t_lightweight_deletes;
