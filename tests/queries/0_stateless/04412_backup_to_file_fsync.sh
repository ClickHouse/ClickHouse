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

# Run a BACKUP with the given destination and settings, then print whether it fsynced every file.
# The oracle is exact rather than a lower bound: a plain backup writes one data file per entry it
# actually stored plus the .backup manifest, so FileSync must equal num_entries + 1. That fails if
# any single data file is left unsynced, which a "more than one file was synced" bound would miss.
# num_entries, not num_files: num_files also counts the entries that were skipped as empty, as
# already present in the base backup, or as being written by another host, and a file that is
# never written is never synced. How many entries get skipped depends on the randomized
# merge-tree settings, so the difference is not constant.
# An archive is packed into one file, so there FileSync must be exactly 1.
# $1 = label, $2 = destination expression, $3 = 'archive' for archives, $4 = extra SETTINGS
#
# The directory oracle is exact for an archive, because an archive is a single file written directly
# into the backup area, which makes that area the only directory whose entry changes. For a File
# destination that is the backup area plus each of its ancestors: an ancestor that already exists is
# not necessarily durable, since a concurrent backup may have created it without having fsynced its
# own parent yet, so the writer walks all of them. That count depends on where the data directory
# lives, so it is derived below rather than hard-coded. For a Disk destination it is 1, the disk root
# - a disk has no configured parent to walk into.
# These are absolute anchors: they fail at a smaller count if the backup area or one of its ancestors
# is not synced, and at a larger one if the walk runs past the area towards the leaves. The plain
# destinations keep a "> 0" assertion because their exact count follows the part layout, which the
# randomized merge-tree settings change; the two assertions further below pin their counts relative
# to these.
# $5 = expected DirectorySync for an archive, $6 = label for it (defaults to the value itself)
check_backup() {
    local label="$1" dest="$2" kind="$3" extra="$4" archive_dirs="${5:-}" archive_label="${6:-${5:-}}"
    local qid="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}"
    local expected="num_entries + 1" dirs="q.ProfileEvents['DirectorySync'] > 0" dirs_label="dir_sync>0="
    if [ "$kind" = archive ]; then
        expected="1"
        dirs="q.ProfileEvents['DirectorySync'] = $archive_dirs"
        dirs_label="dir_sync=$archive_label="
    fi
    $CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$qid" \
        -q "BACKUP TABLE t TO $dest ${extra:+SETTINGS $extra}"
    $CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT '$label every_file_synced=', q.ProfileEvents['FileSync'] = ($expected), ', $dirs_label', $dirs
        FROM system.query_log AS q JOIN system.backups AS b ON q.query_id = b.query_id
        WHERE q.type = 'QueryFinish' AND q.current_database = '$CLICKHOUSE_DATABASE' AND q.query_id = '$qid';
    "
}

# The File archive count is the backup area plus each of its ancestors, so derive it from where the
# area actually is instead of hard-coding a depth. config.xml configures backups.allowed_path as the
# relative "backups", which is resolved against the server's data directory.
data_dir=$($CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT path FROM system.disks WHERE name = 'default'")
allowed_path="${data_dir%/}/backups"
# One fsync per component of an absolute path, i.e. the area itself plus each ancestor up to '/'.
file_archive_dirs=$(( $(printf '%s' "$allowed_path" | tr -cd '/' | wc -c) + 1 ))

# Every File/Disk destination must fsync every written file and the containing directories.
# The "file_default" case omits the setting to verify the default is fsync-on.
check_backup "file_default"  "File('${CLICKHOUSE_TEST_UNIQUE_NAME}_file_def')"     plain   ""
check_backup "file"          "File('${CLICKHOUSE_TEST_UNIQUE_NAME}_file')"         plain   "fsync_backup_files = 1"
check_backup "file_archive"  "File('${CLICKHOUSE_TEST_UNIQUE_NAME}_file.zip')"     archive "fsync_backup_files = 1" "$file_archive_dirs" "area+ancestors"
check_backup "disk"          "Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_disk')"     plain   "fsync_backup_files = 1"
check_backup "disk_archive"  "Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_disk.zip')" archive "fsync_backup_files = 1" 1

dir_sync_of() {
    $CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT ProfileEvents['DirectorySync']
        FROM system.query_log
        WHERE type = 'QueryFinish' AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$1';
    "
}

# A nested destination such as File('a/b/c') also creates the intermediate directories, whose
# entries are durable only once their own parent directory is fsynced. Backing up the same table
# two levels deeper must therefore fsync exactly two directories more than the flat destination:
# the tree below the backup root is identical and the extra two are those intermediate directories.
qid_nested="${CLICKHOUSE_TEST_UNIQUE_NAME}_nested"
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$qid_nested" \
    -q "BACKUP TABLE t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}_n/sub/deep') SETTINGS fsync_backup_files = 1"
echo -e "nested dir_sync - flat dir_sync = 2:\t$(( $(dir_sync_of "$qid_nested") - $(dir_sync_of "${CLICKHOUSE_TEST_UNIQUE_NAME}_file") == 2 ))"

# An ancestor that already exists is not necessarily durable: a concurrent backup may have created
# it without having fsynced its parent yet. Two backups sharing one intermediate directory must
# therefore fsync the same number of directories - the second must not stop at the shared ancestor.
# This holds by construction now that the walk does not sample what already exists.
for i in 1 2; do
    $CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_shared$i" \
        -q "BACKUP TABLE t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}_shared/b$i') SETTINGS fsync_backup_files = 1"
done
echo -e "shared ancestor synced by both:\t$(( $(dir_sync_of "${CLICKHOUSE_TEST_UNIQUE_NAME}_shared2") == $(dir_sync_of "${CLICKHOUSE_TEST_UNIQUE_NAME}_shared1") ))"

# The plain Disk assertion above is only a lower bound, so it also holds if the writer recorded the
# destination root alone and never descended to the directories actually holding the files. Each part
# lives in its own directory, so a table with two more parts must fsync exactly two directories more.
# That difference is zero unless the ancestors of every written file are recorded. Merges would change
# the part count, so they are stopped; the two tables are otherwise identical, which is what makes the
# difference exactly the extra part directories rather than a property of the payload.
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    DROP TABLE IF EXISTS p1 SYNC;
    DROP TABLE IF EXISTS p3 SYNC;
    CREATE TABLE p1 (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE p3 (id UInt64) ENGINE = MergeTree ORDER BY id;
    SYSTEM STOP MERGES p1;
    SYSTEM STOP MERGES p3;
    INSERT INTO p1 SELECT number FROM numbers(1000);
    INSERT INTO p3 SELECT number FROM numbers(1000);
    INSERT INTO p3 SELECT number + 1000 FROM numbers(1000);
    INSERT INTO p3 SELECT number + 2000 FROM numbers(1000);
"
for n in 1 3; do
    $CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_parts$n" \
        -q "BACKUP TABLE p$n TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_parts$n') SETTINGS fsync_backup_files = 1"
done
echo -e "disk 3-part dir_sync - 1-part dir_sync = 2:\t$(( $(dir_sync_of "${CLICKHOUSE_TEST_UNIQUE_NAME}_parts3") - $(dir_sync_of "${CLICKHOUSE_TEST_UNIQUE_NAME}_parts1") == 2 ))"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "DROP TABLE p1 SYNC; DROP TABLE p3 SYNC;"

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
