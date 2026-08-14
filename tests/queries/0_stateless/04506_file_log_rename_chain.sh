#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: the FileLog engine is built only on Linux and macOS (USE_FILELOG), so it is
# absent from the other fast-test builds where `ENGINE = FileLog` would fail with UNKNOWN_STORAGE.
# Tag no-parallel: FileLog -> MV streaming latency depends on `BackgroundSchedulePool` scheduling;
# under heavy parallel load `wait_for_row_count` can drift past its timeout. Same precedent as
# `02968_file_log_multiple_read.sh`.
#
# Regression test for a logrotate-style rename chain processed in ONE `updateFileInfos` batch:
#   `app.log.1 -> app.log.2`, `app.log -> app.log.1`, then a fresh `app.log`.
# The renamed files must keep their read offsets (no already-consumed rows re-read). On Linux
# inotify delivers the renames in chronological order; the Darwin watcher must reconstruct that
# order (a topological sort of the `DW_ITEM_MOVED_TO` events) so that reusing the name `app.log.1`
# does not drop the still-live inode's meta and re-read it from offset 0.
#
# The consumer (materialized view) is detached across the rotation and re-attached afterwards, so
# the whole chain is drained by a single `updateFileInfos` call, independent of watcher poll timing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}

rm -rf "${logs_dir}"
mkdir -p "${logs_dir}/"

printf '1\n2\n3\n' > "${logs_dir}/app.log"
printf '10\n20\n30\n' > "${logs_dir}/app.log.1"

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

# Stream the two initial files (6 rows) and let the cycle persist their on-disk meta.
wait_for_row_count 6
sleep 2

# Detach the consumer so the rename chain accumulates into a single drain on re-attach.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE file_log_mv"
sleep 2

# Rotate: the oldest first, then shift, then a fresh current file.
mv "${logs_dir}/app.log.1" "${logs_dir}/app.log.2"
mv "${logs_dir}/app.log" "${logs_dir}/app.log.1"
printf '100\n200\n' > "${logs_dir}/app.log"
sleep 3

${CLICKHOUSE_CLIENT} --query "ATTACH TABLE file_log_mv"
sleep 5

# Only the fresh app.log (100, 200) is new; the two rotated files keep their offsets, so the sink
# must contain exactly the initial six rows plus the two new ones, with no duplicates.
wait_for_row_count 8
${CLICKHOUSE_CLIENT} --query "SELECT id FROM sink ORDER BY id"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE file_log_mv;
DROP TABLE file_log;
DROP TABLE sink;
"

rm -rf "${logs_dir}"
