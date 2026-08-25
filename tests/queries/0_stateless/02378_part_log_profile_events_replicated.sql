-- Tags: long, replica, no-replicated-database, no-parallel, no-shared-merge-tree
-- no-shared-merge-tree: depend on events for replicatied merge tree

DROP TABLE IF EXISTS part_log_profile_events_r1 SYNC;
DROP TABLE IF EXISTS part_log_profile_events_r2 SYNC;

CREATE TABLE part_log_profile_events_r1 (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02378/part_log_profile_events', 'r1')
ORDER BY x
PARTITION BY x >= 128
;

CREATE TABLE part_log_profile_events_r2 (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02378/part_log_profile_events', 'r2')
ORDER BY x
PARTITION BY x >= 128
;

-- SYSTEM STOP MERGES part_log_profile_events_r1;
-- SYSTEM STOP MERGES part_log_profile_events_r2;

SET max_block_size = 64, max_insert_block_size = 64, min_insert_block_size_rows = 64;

INSERT INTO part_log_profile_events_r1 SELECT number FROM numbers(1000);

SYSTEM SYNC REPLICA part_log_profile_events_r2;

SYSTEM FLUSH LOGS part_log;

SELECT
    count() > 1
    AND SUM(ProfileEvents['ZooKeeperTransactions']) >= 4
FROM system.part_log
WHERE event_time > now() - INTERVAL 10 MINUTE
    AND database == currentDatabase() AND table == 'part_log_profile_events_r2'
    AND event_type == 'DownloadPart'
;

DROP TABLE part_log_profile_events_r1 SYNC;
DROP TABLE part_log_profile_events_r2 SYNC;
