-- Tags: no-ordinary-database, no-shared-merge-tree, no-async-insert
-- no-ordinary-database: `implicit_transaction` needs an Atomic database.
-- no-shared-merge-tree: the byte counts below need the parts on the local disk.
-- no-async-insert: an async INSERT writes its part in the queue flush, not in the INSERT query, and is rejected in a transaction.
-- Random settings limits: min_bytes_for_full_part_storage=(0, 0)

-- Every file an INSERT writes for a part (`partition.dat`, the min-max index, projection parts, `txn_version.txt`)
-- goes through the query's own local write throttler: with `max_local_write_bandwidth` set it counts every byte the
-- INSERT wrote, and with `max_local_write_bandwidth = 0` no per-query throttler counts any.

DROP TABLE IF EXISTS t_part_files;
DROP TABLE IF EXISTS t_part_files_txn;

CREATE TABLE t_part_files
(
    k UInt64,
    v String,
    PROJECTION by_v (SELECT k, v ORDER BY v)
)
ENGINE = MergeTree
PARTITION BY k % 2
ORDER BY k
SETTINGS storage_policy = 'default', enable_vertical_merge_algorithm = 0;

CREATE TABLE t_part_files_txn (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = 'default';

SYSTEM STOP MERGES t_part_files;

INSERT INTO t_part_files SETTINGS max_local_write_bandwidth = 1000000000000, log_comment = 'insert, throttled'
    SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_part_files SETTINGS max_local_write_bandwidth = 0, log_comment = 'insert, unthrottled'
    SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_part_files_txn SETTINGS implicit_transaction = 1, max_local_write_bandwidth = 1000000000000, log_comment = 'transaction, throttled'
    SELECT number FROM numbers(1000);
INSERT INTO t_part_files_txn SETTINGS implicit_transaction = 1, max_local_write_bandwidth = 0, log_comment = 'transaction, unthrottled'
    SELECT number FROM numbers(1000);

SYSTEM START MERGES t_part_files;
OPTIMIZE TABLE t_part_files FINAL;

SYSTEM FLUSH LOGS query_log, part_log;

-- `throttled`: bytes written past the query's throttler. `unthrottled`: bytes a per-query throttler counted anyway.
SELECT
    log_comment,
    ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] > 0,
    if(endsWith(log_comment, ', throttled'),
        toInt64(ProfileEvents['WriteBufferFromFileDescriptorWriteBytes']) - toInt64(ProfileEvents['QueryLocalWriteThrottlerBytes']),
        toInt64(ProfileEvents['QueryLocalWriteThrottlerBytes']))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment IN ('insert, throttled', 'insert, unthrottled', 'transaction, throttled', 'transaction, unthrottled')
ORDER BY log_comment;

-- A merge writes the whole merged part with its own settings: its throttler, if it has one, counts every byte.
-- Vertical merges are disabled above: they also write a temporary file and the gathered columns with other settings.
SELECT
    count() > 0,
    countIf(ProfileEvents['QueryLocalWriteThrottlerBytes'] != 0
        AND ProfileEvents['QueryLocalWriteThrottlerBytes'] != ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_part_files' AND event_type = 'MergeParts';

DROP TABLE t_part_files;
DROP TABLE t_part_files_txn;
