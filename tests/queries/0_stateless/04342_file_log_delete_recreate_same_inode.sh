#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: FileLog -> MV streaming latency depends on `BackgroundSchedulePool`
# scheduling; under heavy parallel load `wait_for_row_count` can drift past its
# timeout. Same precedent as `02968_file_log_multiple_read.sh`.
#
# Regression test for a FileLog stale-metadata crash when a file is deleted and
# recreated reusing the SAME inode within one watcher backoff window, so the
# `DW_ITEM_REMOVED` and `DW_ITEM_ADDED` events are processed together. The
# REMOVED cleanup is skipped (the recreate flips the status back to OPEN before
# the removal loop runs) and `onFileAppeared`'s inode-change guard does not fire
# (the inode is unchanged), so the on-disk meta with the old, larger offset is
# left behind while the in-memory offset is reset to 0. `serialize()` then read
# the larger stored offset and aborted the server with a LOGICAL_ERROR:
#   Last stored last_written_position in meta file a.csv is bigger than current
#   last_written_pos (N > 0)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}

rm -rf "${logs_dir}"
mkdir -p "${logs_dir}/"

echo 1 >> "${logs_dir}/a.csv"
echo 2 >> "${logs_dir}/a.csv"
echo 3 >> "${logs_dir}/a.csv"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS file_log;
DROP TABLE IF EXISTS sink;
DROP TABLE IF EXISTS file_log_mv;

CREATE TABLE file_log (
    id Int64
) ENGINE = FileLog('${logs_dir}/', 'CSV')
SETTINGS poll_directory_watch_events_backoff_init = 1000,
         poll_directory_watch_events_backoff_max = 1000;

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

wait_for_row_count 3

# Delete and immediately recreate `a.csv` with no gap so both the REMOVED and the
# ADDED events queue inside the same backoff window. With the high backoff
# (1000 ms) above the watcher drains both together. On a typical filesystem
# (ext4) the immediate recreate reuses the just-freed inode, which is the
# condition that triggered the crash. Repeat a few rounds because inode reuse
# is not guaranteed on every filesystem/iteration; one reuse round is enough to
# crash an unfixed server. After the fix the server must stay up and stream the
# new content of every round.
for round in $(seq 1 5); do
    rm -f "${logs_dir}/a.csv"; printf '100\n200\n' > "${logs_dir}/a.csv"
    # Let the watcher deliver and process the REMOVED+ADDED batch.
    sleep 2
    if ! ${CLICKHOUSE_CLIENT} --query "SELECT 1" >/dev/null 2>&1; then
        echo "server is not responding after round ${round} (likely crashed)"
        exit 1
    fi
    # Restore the original content for the next round's delete to be meaningful.
    printf '1\n2\n3\n' > "${logs_dir}/a.csv"
    sleep 2
done

# The server survived every delete+recreate round.
${CLICKHOUSE_CLIENT} --query "SELECT 'ok'"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE file_log_mv;
DROP TABLE file_log;
DROP TABLE sink;
"

rm -rf "${logs_dir}"
