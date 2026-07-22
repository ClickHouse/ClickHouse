#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: FileLog -> MV streaming latency depends on `BackgroundSchedulePool`
# scheduling; under heavy parallel load `wait_for_row_count` can drift past its
# timeout. Same precedent as `02968_file_log_multiple_read.sh`.
#
# Regression test for FileLog stale metadata under a rename+recreate batch.
# Within one `poll_directory_watch_events_backoff_max` window we rename
# `a.csv` (inode 42) to `b.csv` and recreate `a.csv` with a fresh inode.
# Inotify emits, in chronological order, `IN_MOVED_FROM(a)`,
# `IN_MOVED_TO(b)`, `IN_CREATE(a)`. When events were aggregated per-name
# in an `unordered_map`, `updateFileInfos` could process the `a` key first
# and find `meta_by_inode[42].file_name == "a"` (because the rename pair
# for `b` had not been observed yet) — the ownership-guarded cleanup then
# erased `meta_by_inode[42]` and removed `a.bin` on disk. The subsequent
# `MOVED_TO(b)` re-created `meta_by_inode[42]` with zero offsets, so `b`
# was re-read from the start and the previously-consumed rows
# `1, 2, 3` were emitted again as duplicates.

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

# Rename `a.csv` to `b.csv` and immediately recreate `a.csv` with a fresh
# inode, appending unique markers to both. All three filesystem events
# must land in the same watcher backoff window for this to exercise the
# cross-name event-ordering hazard.
mv "${logs_dir}/a.csv" "${logs_dir}/b.csv"
echo 5 >> "${logs_dir}/b.csv"
echo 4 >> "${logs_dir}/a.csv"
echo 6 >> "${logs_dir}/a.csv"

# Expected total (no duplicates): 1, 2, 3 (consumed before rename)
#                              + 5 (appended to renamed b.csv)
#                              + 4, 6 (new a.csv).
wait_for_row_count 6

${CLICKHOUSE_CLIENT} --query "SELECT count(), countDistinct(id) FROM sink;"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM sink ORDER BY id;"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE file_log_mv;
DROP TABLE file_log;
DROP TABLE sink;
"

rm -rf "${logs_dir}"
