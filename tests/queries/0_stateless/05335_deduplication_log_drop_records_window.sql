-- Tags: no-async-insert, no-random-merge-tree-settings
-- no-async-insert: async inserts compute deduplication block ids differently

-- `ALTER TABLE ... DROP PART` writes a DROP record to the deduplication log for every block id it removes from
-- the window. Those records must not count toward `non_replicated_deduplication_window` when old logs are
-- deleted: a log full of ADD + DROP pairs adds nothing to the window, and the older log that holds the ADD
-- records of the block ids still in the window must stay. Otherwise a block id that was deduplicated before
-- `DETACH` / `ATTACH` is accepted again afterwards.

DROP TABLE IF EXISTS t_dedup_log_drop_records;

CREATE TABLE t_dedup_log_drop_records (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS non_replicated_deduplication_window = 2;

SYSTEM STOP MERGES t_dedup_log_drop_records;

-- 4 records fill the first log (`rotate_interval` is twice the window); the window is {3, 4}.
INSERT INTO t_dedup_log_drop_records VALUES (1);
INSERT INTO t_dedup_log_drop_records VALUES (2);
INSERT INTO t_dedup_log_drop_records VALUES (3);
INSERT INTO t_dedup_log_drop_records VALUES (4);

-- The second log gets ADD 5, DROP 5, ADD 6, DROP 6: four records, and the window is {4} again.
INSERT INTO t_dedup_log_drop_records VALUES (5);
ALTER TABLE t_dedup_log_drop_records DROP PART 'all_5_5_0';
INSERT INTO t_dedup_log_drop_records VALUES (6);
ALTER TABLE t_dedup_log_drop_records DROP PART 'all_6_6_0';

SELECT count() FROM t_dedup_log_drop_records;
INSERT INTO t_dedup_log_drop_records VALUES (4);
SELECT count() FROM t_dedup_log_drop_records;

-- The window is rebuilt from the logs on disk, and block 4 must still be in it.
DETACH TABLE t_dedup_log_drop_records;
ATTACH TABLE t_dedup_log_drop_records;
INSERT INTO t_dedup_log_drop_records VALUES (4);
SELECT count() FROM t_dedup_log_drop_records;
SELECT x, count() FROM t_dedup_log_drop_records GROUP BY x HAVING count() > 1;

DROP TABLE t_dedup_log_drop_records;
