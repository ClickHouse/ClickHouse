#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: FileLog -> MV streaming latency depends on `BackgroundSchedulePool`
# scheduling; under heavy parallel load `wait_for_row_count` can drift past its
# timeout. Same precedent as `02968_file_log_multiple_read.sh`.
#
# Regression test for FileLog stale metadata when a file is deleted and recreated.
# Previously, recreating a file with a different inode under the same name left
# the old inode's entry in `meta_by_inode` and the on-disk `<name>.bin` meta file
# in place, so on restart the FileLog could end up skipping content of the new
# file or otherwise observing stale offsets.

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
SETTINGS poll_directory_watch_events_backoff_max = 1000;

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

# Delete the file and recreate it with new content. The new file gets a fresh
# inode; previously, the stale `meta_by_inode` entry for the old inode kept
# pointing at the same filename, leaving two entries with `file_name = "a.csv"`
# and the on-disk meta file with old offsets. After the fix, the old inode's
# meta is cleaned up when the recreated file is observed via DW_ITEM_ADDED.
rm "${logs_dir}/a.csv"
sleep 2
echo 100 >> "${logs_dir}/a.csv"
echo 200 >> "${logs_dir}/a.csv"

wait_for_row_count 5

${CLICKHOUSE_CLIENT} --query "SELECT id FROM sink ORDER BY id;"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE file_log_mv;
DROP TABLE file_log;
DROP TABLE sink;
"

rm -rf "${logs_dir}"
