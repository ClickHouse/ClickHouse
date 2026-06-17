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
# REUSES THE SAME INODE within one watcher backoff window. The same-inode
# recreate is forced deterministically and does NOT rely on filesystem
# inode-allocation policy: a hard link outside the watched directory pins the
# inode across the unlink, the content is rewritten through that held link, then
# the inode is linked back under the original name. This yields a
# `DW_ITEM_REMOVED` + `DW_ITEM_ADDED` batch for the same name and the same inode.

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
SETTINGS poll_directory_watch_events_backoff_init = 5000,
         poll_directory_watch_events_backoff_max = 5000;

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

# Pin the inode, delete the watched name, rewrite the content through the held
# link, then link the same inode back. The watcher sees REMOVED + ADDED for
# `a.csv` with an UNCHANGED inode in one 5s backoff window. With the bug the
# in-memory offset is reset to 0 while the on-disk meta keeps 6, so the next
# serialize() aborts the server with a LOGICAL_ERROR.
ln "${logs_dir}/a.csv" "${held}"
rm -f "${logs_dir}/a.csv"
printf '100\n200\n' > "${held}"
ln "${held}" "${logs_dir}/a.csv"

# One backoff window plus margin for the watcher to drain and process the batch.
sleep 8
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
