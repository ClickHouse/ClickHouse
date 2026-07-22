#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-random-merge-tree-settings, no-replicated-database, no-shared-merge-tree
# no-fasttest: the encrypted case below needs the encrypted disk type, which is built only with SSL.
# no-object-storage: object storage does not fsync file contents (the fix is gated on !isRemote()).
# no-random-merge-tree-settings: the test asserts on FileSync counts, which depend on the part layout.
# no-replicated-database, no-shared-merge-tree: the encrypted case below pins a custom local disk.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111321
# RESTORE must fsync the restored part file contents when the table enables fsync_after_insert,
# otherwise a power loss right after RESTORE returns leaves the parts torn and the table empty.
# We assert the per-query FileSync ProfileEvent on the RESTORE query row (parallel-safe: filtered
# by query_id + current_database). RESTORE performs a couple of fsyncs on backup-side metadata
# regardless, so the discriminating signal is "were all of the part's data files fsynced", i.e.
# FileSync >= number of files in the restored part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_restore_fsync_on;
    DROP TABLE IF EXISTS t_restore_fsync_off;

    CREATE TABLE t_restore_fsync_on (id UInt64, s String) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_after_insert = 1, fsync_part_directory = 1, min_bytes_for_wide_part = 0;
    INSERT INTO t_restore_fsync_on SELECT number, toString(number) FROM numbers(1000);

    CREATE TABLE t_restore_fsync_off (id UInt64, s String) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_after_insert = 0, fsync_part_directory = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_restore_fsync_off SELECT number, toString(number) FROM numbers(1000);
"

# Number of files in the (single) part - the restored part has the same file set.
files_in_part=$($CLICKHOUSE_CLIENT -q "SELECT files FROM system.parts WHERE database = currentDatabase() AND table = 't_restore_fsync_on' AND active")

$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_restore_fsync_on TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_on')" > /dev/null
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_restore_fsync_off TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_off')" > /dev/null
$CLICKHOUSE_CLIENT -m -q "DROP TABLE t_restore_fsync_on SYNC; DROP TABLE t_restore_fsync_off SYNC;"

qid_on="restore-on-$CLICKHOUSE_DATABASE"
qid_off="restore-off-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id "$qid_on" -q "RESTORE TABLE t_restore_fsync_on FROM Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_on')" > /dev/null
$CLICKHOUSE_CLIENT --query_id "$qid_off" -q "RESTORE TABLE t_restore_fsync_off FROM Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_off')" > /dev/null

# Data must be intact after restore.
echo "count on:  $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_restore_fsync_on")"
echo "count off: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_restore_fsync_off")"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# With fsync_after_insert=1 every file in the restored part must be fdatasynced, so the RESTORE row
# reports FileSync >= number of files in the part. Before the fix no part file was synced (FileSync
# only reflected a couple of backup-side metadata syncs, far below the file count).
$CLICKHOUSE_CLIENT --param_query_id "$qid_on" --param_files "$files_in_part" -q "
    SELECT 'restore with fsync_after_insert=1, all part files fsynced: ',
           ProfileEvents['FileSync'] >= {files:UInt64}
    FROM system.query_log
    WHERE query_id = {query_id:String} AND type = 'QueryFinish' AND current_database = currentDatabase()
    ORDER BY event_time_microseconds DESC LIMIT 1"

# With fsync_after_insert=0 the part files must NOT be fsynced, so FileSync stays below the file count.
$CLICKHOUSE_CLIENT --param_query_id "$qid_off" --param_files "$files_in_part" -q "
    SELECT 'restore with fsync_after_insert=0, all part files fsynced: ',
           ProfileEvents['FileSync'] >= {files:UInt64}
    FROM system.query_log
    WHERE query_id = {query_id:String} AND type = 'QueryFinish' AND current_database = currentDatabase()
    ORDER BY event_time_microseconds DESC LIMIT 1"

$CLICKHOUSE_CLIENT -m -q "DROP TABLE t_restore_fsync_on SYNC; DROP TABLE t_restore_fsync_off SYNC;"

# Encrypted incremental restore: files that come entirely from the base backup are copied via
# getBaseBackup()->copyFileToDisk(..., sync). That branch must forward the encrypted read (else the
# restore fails with CANNOT_RESTORE_TO_NONENCRYPTED_DISK) and still fsync the files when requested.
enc_disk="disk(type = encrypted, disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_enc/'), key = '1234567812345678')"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_restore_fsync_enc;
    CREATE TABLE t_restore_fsync_enc (id UInt64, s String) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_after_insert = 1, fsync_part_directory = 1, min_bytes_for_wide_part = 0, disk = $enc_disk;
    INSERT INTO t_restore_fsync_enc SELECT number, toString(number) FROM numbers(1000);
"
enc_files=$($CLICKHOUSE_CLIENT -q "SELECT files FROM system.parts WHERE database = currentDatabase() AND table = 't_restore_fsync_enc' AND active")

# Full backup, then an unchanged incremental backup so every file is served by the base backup.
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_restore_fsync_enc TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_enc_base')" > /dev/null
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_restore_fsync_enc TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_enc_incr') SETTINGS base_backup = Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_enc_base')" > /dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE t_restore_fsync_enc SYNC"

qid_enc="restore-enc-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id "$qid_enc" -q "RESTORE TABLE t_restore_fsync_enc FROM Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_enc_incr') SETTINGS base_backup = Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_enc_base')" > /dev/null

# Before the fix the restore threw and the table stayed empty; now it restores every row.
echo "encrypted incremental count: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_restore_fsync_enc")"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --param_query_id "$qid_enc" --param_files "$enc_files" -q "
    SELECT 'encrypted incremental restore with fsync_after_insert=1, all part files fsynced: ',
           ProfileEvents['FileSync'] >= {files:UInt64}
    FROM system.query_log
    WHERE query_id = {query_id:String} AND type = 'QueryFinish' AND current_database = currentDatabase()
    ORDER BY event_time_microseconds DESC LIMIT 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_restore_fsync_enc SYNC"
