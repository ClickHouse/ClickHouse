#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-random-merge-tree-settings, no-replicated-database, no-shared-merge-tree
# no-fasttest: the encrypted case below needs the encrypted disk type, which is built only with SSL.
# no-object-storage: object storage does not fsync file contents (the fix is gated on !isRemote()).
# no-random-merge-tree-settings: the test asserts on FileSync counts, which depend on the part layout.
# no-replicated-database, no-shared-merge-tree: the encrypted case below pins a custom local disk.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111321
# RESTORE must fsync the restored part file contents when the table enables fsync_after_insert,
# otherwise a power loss right after RESTORE returns leaves the parts torn and the table empty.
# We assert on the RESTORE query's FileSync ProfileEvent (parallel-safe: filtered by query_id +
# current_database). RESTORE also performs a few backup-side FileSync events unrelated to the part
# files, so the discriminating signal is the on-vs-off FileSync DELTA: that constant backup-side
# noise cancels out, and the remaining delta must cover every physical file of the restored part
# (an empty Array column contributes a zero-byte .bin, which INSERT fsyncs too).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `arr` is Array(UInt32) left empty for every row, so its `.bin` is a required zero-byte part file -
# INSERT fsyncs it, so RESTORE must too. Two tables with identical data, differing only in fsync_after_insert.
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_restore_fsync_on;
    DROP TABLE IF EXISTS t_restore_fsync_off;

    CREATE TABLE t_restore_fsync_on (id UInt64, s String, arr Array(UInt32)) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_after_insert = 1, fsync_part_directory = 1, min_bytes_for_wide_part = 0;
    INSERT INTO t_restore_fsync_on SELECT number, toString(number), [] FROM numbers(1000);

    CREATE TABLE t_restore_fsync_off (id UInt64, s String, arr Array(UInt32)) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_after_insert = 0, fsync_part_directory = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_restore_fsync_off SELECT number, toString(number), [] FROM numbers(1000);
"

# Count the physical files RESTORE actually copies (and therefore must fsync). This is the real target,
# larger than system.parts.files (= checksums entries) - it includes checksums.txt, columns.txt and the
# zero-byte arr.bin - but excludes the version-metadata files RESTORE deliberately skips (see
# restorePartFromBackup: txn_version.txt[.tmp] and metadata_version.txt are not copied). The restored
# part has the same on-disk file set.
part_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_restore_fsync_on' AND active")
copied_files=$(find "$part_path" -type f \
    ! -name 'txn_version.txt' ! -name 'txn_version.txt.tmp' ! -name 'metadata_version.txt' | wc -l)
# Sanity: there is a required zero-byte file in the part (the empty Array's .bin), which INSERT fsyncs too.
zero_byte_files=$(find "$part_path" -type f -size 0 | wc -l)
echo "has zero-byte part file: $([ "$zero_byte_files" -ge 1 ] && echo 1 || echo 0)"

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

# The on/off FileSync delta cancels the constant backup-side syncs that both restores perform and
# isolates the restored part-file syncs. With fsync_after_insert=1 every restore-copied file (incl. the
# zero-byte arr.bin) is fsynced, so the delta must be >= the number of files restore copied. Before the
# fix no part file was synced and the delta was ~0.
$CLICKHOUSE_CLIENT --param_qid_on "$qid_on" --param_qid_off "$qid_off" --param_copied "$copied_files" -q "
    WITH
        (SELECT ProfileEvents['FileSync'] FROM system.query_log
         WHERE query_id = {qid_on:String} AND type = 'QueryFinish' AND current_database = currentDatabase()
         ORDER BY event_time_microseconds DESC LIMIT 1) AS sync_on,
        (SELECT ProfileEvents['FileSync'] FROM system.query_log
         WHERE query_id = {qid_off:String} AND type = 'QueryFinish' AND current_database = currentDatabase()
         ORDER BY event_time_microseconds DESC LIMIT 1) AS sync_off
    SELECT 'restore fsync delta covers all part files: ', (toInt64(sync_on) - toInt64(sync_off)) >= {copied:UInt64}"

$CLICKHOUSE_CLIENT -m -q "DROP TABLE t_restore_fsync_on SYNC; DROP TABLE t_restore_fsync_off SYNC;"

# Encrypted incremental restore: files that come entirely from the base backup are copied via
# getBaseBackup()->copyFileToDisk(..., sync). That branch must forward the encrypted read (else the
# restore fails with CANNOT_RESTORE_TO_NONENCRYPTED_DISK) and still fsync the files when requested.
enc_disk="disk(type = encrypted, disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_enc/'), key = '1234567812345678')"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_restore_fsync_enc;
    CREATE TABLE t_restore_fsync_enc (id UInt64, s String, arr Array(UInt32)) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_after_insert = 1, fsync_part_directory = 1, min_bytes_for_wide_part = 0, disk = $enc_disk;
    INSERT INTO t_restore_fsync_enc SELECT number, toString(number), [] FROM numbers(1000);
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
