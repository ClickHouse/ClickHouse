#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - BACKUP TO File/Disk requires the backups path/disk configured in CI

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111320:
# BACKUP ... TO File()/Disk() must fsync the data files, the .backup manifest and the
# containing directories (so an acknowledged backup survives power loss) when
# fsync_backup_files=1, and must issue no fsync when fsync_backup_files=0. We assert this via
# the FileSync / DirectorySync ProfileEvents recorded for the BACKUP query in
# system.query_log, across the File and Disk engines and plain and archive destinations.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

client_opts=(--send_logs_level 'error')

$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    DROP TABLE IF EXISTS t SYNC;
    CREATE TABLE t (id UInt64, key String, v UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t SELECT number, concat('k', toString(number % 7)), number FROM numbers(50000);
"

# Run a BACKUP with the given destination and settings, then print whether it issued fsyncs.
# For a plain (non-archive) backup of a table with several files we assert FileSync > 1: the
# manifest alone would give FileSync = 1, so > 1 proves the individual data files were synced
# too (not just the manifest). Archives are a single file, so we only require FileSync > 0.
# $1 = label, $2 = destination expression, $3 = min FileSync (1 => assert >0, 2 => assert >1),
# $4 = extra SETTINGS (may be empty)
check_backup() {
    local label="$1" dest="$2" min_file_sync="$3" extra="$4"
    local qid="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}"
    $CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$qid" \
        -q "BACKUP TABLE t TO $dest ${extra:+SETTINGS $extra}"
    $CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT '$label file_sync>=$min_file_sync=', ProfileEvents['FileSync'] >= $min_file_sync, ', dir_sync>0=', ProfileEvents['DirectorySync'] > 0
        FROM system.query_log
        WHERE type = 'QueryFinish' AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$qid';
    "
}

# Every File/Disk destination must fsync files and directories. Plain backups must sync more
# than one file (data files + manifest); archives are a single synced file.
# The "file_default" case omits the setting to verify the default is fsync-on.
check_backup "file_default"  "File('${CLICKHOUSE_TEST_UNIQUE_NAME}_file_def')"     2 ""
check_backup "file"          "File('${CLICKHOUSE_TEST_UNIQUE_NAME}_file')"         2 "fsync_backup_files = 1"
check_backup "file_archive"  "File('${CLICKHOUSE_TEST_UNIQUE_NAME}_file.zip')"     1 "fsync_backup_files = 1"
check_backup "disk"          "Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_disk')"     2 "fsync_backup_files = 1"
check_backup "disk_archive"  "Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_disk.zip')" 1 "fsync_backup_files = 1"

# With fsync_backup_files=0 no fsync is issued (opt-out, matches the pre-fix behavior).
qid_off="${CLICKHOUSE_TEST_UNIQUE_NAME}_off"
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$qid_off" \
    -q "BACKUP TABLE t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}_off') SETTINGS fsync_backup_files = 0"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'off file_sync=0=', ProfileEvents['FileSync'] = 0, ', dir_sync=0=', ProfileEvents['DirectorySync'] = 0
    FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$qid_off';
"

# The backup must still restore correctly (durability change must not corrupt the backup).
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    DROP TABLE t SYNC;
    RESTORE TABLE t FROM File('${CLICKHOUSE_TEST_UNIQUE_NAME}_file') FORMAT Null;
    SELECT 'restored count: ', count() FROM t;
    DROP TABLE t SYNC;
"
