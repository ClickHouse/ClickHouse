-- Tags: no-object-storage, no-random-merge-tree-settings
-- no-object-storage: object storage has no fsync, so FileSync stays 0 there.

-- An INSERT whose block is deduplicated is acknowledged on the strength of the part that already
-- holds the block. With fsync_after_insert = 1 and fsync_after_insert_each_part = 0 that part may
-- never have been synced (the INSERT that committed it failed or was cancelled before its batched
-- fsync at the end of the query), so the deduplicated INSERT has to sync it before it is
-- acknowledged. Here the existing part is written with fsync_after_insert = 0 to make it unsynced.

SET insert_deduplicate = 1, async_insert = 0;

DROP TABLE IF EXISTS t_fsync_dedup;
DROP TABLE IF EXISTS t_fsync_dedup_replicated;

CREATE TABLE t_fsync_dedup (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, non_replicated_deduplication_window = 100,
    fsync_after_insert = 0, fsync_after_insert_each_part = 0;

CREATE TABLE t_fsync_dedup_replicated (k UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fsync_dedup_replicated', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 0, fsync_after_insert_each_part = 0;

INSERT INTO t_fsync_dedup SETTINGS insert_deduplication_token = 'token' SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_dedup_replicated SETTINGS insert_deduplication_token = 'token' SELECT number, toString(number) FROM numbers(100);

ALTER TABLE t_fsync_dedup MODIFY SETTING fsync_after_insert = 1;
ALTER TABLE t_fsync_dedup_replicated MODIFY SETTING fsync_after_insert = 1;

INSERT INTO t_fsync_dedup SETTINGS insert_deduplication_token = 'token', log_comment = '05394_dedup' SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_dedup_replicated SETTINGS insert_deduplication_token = 'token', log_comment = '05394_dedup_replicated' SELECT number, toString(number) FROM numbers(100);

-- The second INSERT is deduplicated.
SELECT count(), sum(k) FROM t_fsync_dedup;
SELECT count(), sum(k) FROM t_fsync_dedup_replicated;

SYSTEM FLUSH LOGS query_log;

-- But it syncs the existing part.
SELECT log_comment, ProfileEvents['DuplicatedInsertedBlocks'] > 0, ProfileEvents['FileSync'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05394_dedup%'
ORDER BY log_comment;

DROP TABLE t_fsync_dedup;
DROP TABLE t_fsync_dedup_replicated;
