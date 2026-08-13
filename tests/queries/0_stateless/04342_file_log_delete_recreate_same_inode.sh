#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: the FileLog engine is Linux-only (USE_FILELOG is gated on
# OS_LINUX and depends on inotify), so it is absent from the Darwin/fast-test
# build and `ENGINE = FileLog` would fail with UNKNOWN_STORAGE there.
# Tag no-parallel: FileLog -> MV streaming latency depends on `BackgroundSchedulePool`
# scheduling; under heavy parallel load `wait_for_row_count` can drift past its
# timeout. Same precedent as `02968_file_log_multiple_read.sh`.
#
# Regression test for a FileLog stale-metadata crash on delete+recreate that
# REUSES THE SAME INODE and is processed in ONE `updateFileInfos` batch. Two
# independent sources of non-determinism are removed so the bug is exercised
# regardless of the filesystem and the scheduler:
#   1. Same inode: a hard link outside the watched directory pins the inode
#      across the unlink; the content is rewritten through that held link and
#      the inode is linked back under the original name. This does not rely on
#      filesystem inode-allocation policy (tmpfs/xfs never reuse, ext4 may).
#   2. Same drain: the consumer (the materialized view) is detached before the
#      delete/relink and re-attached afterwards. `getEventsAndReset` is only
#      called while a dependent view is attached, so with the view detached the
#      watcher accumulates `DW_ITEM_REMOVED` + `DW_ITEM_ADDED` and they are
#      drained together on re-attach, independent of `watchFunc` poll timing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
# Held outside the watched directory but on the same filesystem (hard links
# cannot cross filesystems) so it generates no watcher events.
held=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}.held

rm -rf "${logs_dir}" "${held}"
mkdir -p "${logs_dir}/"

printf '1\n2\n3\n' > "${logs_dir}/a.csv"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS file_log;
DROP TABLE IF EXISTS sink;
DROP TABLE IF EXISTS file_log_mv;

CREATE TABLE file_log (
    id Int64
) ENGINE = FileLog('${logs_dir}/', 'CSV')
SETTINGS poll_directory_watch_events_backoff_init = 500,
         poll_directory_watch_events_backoff_max = 500;

CREATE TABLE sink (
    id Int64
) ENGINE = MergeTree
ORDER BY id;

CREATE MATERIALIZED VIEW file_log_mv TO sink AS
    SELECT id FROM file_log;
" || exit 1

function count()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM sink;"
}

function wait_for_row_count()
{
    local threshold="$1"
    local timeout=120
    local start=$EPOCHSECONDS
    while [[ $(count) -lt $threshold ]]; do
        if ((EPOCHSECONDS - start > timeout)); then
            echo "Timeout (${timeout}s) waiting for $threshold rows. Got $(count)."
            exit 1
        fi
        sleep 0.5
    done
}

# Stream the initial content and let the cycle persist the on-disk meta
# (last_written_position = 6) that the recreate will contradict.
wait_for_row_count 3
sleep 2

# Detach the consumer so no streaming cycle drains the watcher's event queue
# while we rebuild the file. The settle sleep lets any in-flight cycle finish.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE file_log_mv"
sleep 2

# Pin the inode, delete the watched name, rewrite the content through the held
# link, then link the same inode back. With the consumer detached the watcher
# records REMOVED + ADDED for `a.csv` (same inode) and keeps both queued. The
# settle sleep lets the watcher read both inotify events.
ln "${logs_dir}/a.csv" "${held}"
rm -f "${logs_dir}/a.csv"
printf '100\n200\n' > "${held}"
ln "${held}" "${logs_dir}/a.csv"
sleep 3

# Re-attach the consumer: the next streaming cycle drains the accumulated
# REMOVED + ADDED in one `updateFileInfos` call. With the bug the in-memory
# offset is reset to 0 while the on-disk meta keeps 6, so the next serialize()
# aborts the server with a LOGICAL_ERROR.
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE file_log_mv"

sleep 5
if ! ${CLICKHOUSE_CLIENT} --query "SELECT 1" >/dev/null 2>&1; then
    echo "server is not responding after delete+recreate with reused inode (likely crashed)"
    exit 1
fi

# FileLog must avoid the exception AND stream the recreated content (100, 200).
wait_for_row_count 5
${CLICKHOUSE_CLIENT} --query "SELECT id FROM sink ORDER BY id"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE file_log_mv;
DROP TABLE file_log;
DROP TABLE sink;
"

rm -rf "${logs_dir}" "${held}"
