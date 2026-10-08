#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: this test exercises the FileLog -> MV streaming path which depends on
# `BackgroundSchedulePool` task scheduling latency. Under heavy parallel load (e.g. 50x in
# the flaky check on amd_asan_ubsan), pool contention can push file-detection latency past
# any reasonable timeout. Sequential execution gives consistent ~10s runtime. This matches
# the precedent set by `02026_storage_filelog_largefile.sh` and `04031_filelog_drop_mv_no_exception.sh`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}

rm -rf ${logs_dir}

mkdir -p ${logs_dir}/

for i in {1..10}
do
	echo $i >> ${logs_dir}/a.txt
done

${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS file_log;
DROP TABLE IF EXISTS table_to_store_data;
DROP TABLE IF EXISTS file_log_mv;

CREATE TABLE file_log (
    id Int64
) ENGINE = FileLog('${logs_dir}/', 'CSV')
SETTINGS poll_directory_watch_events_backoff_max = 1000;

CREATE TABLE table_to_store_data (
    id Int64
) ENGINE = MergeTree
ORDER BY id;

CREATE MATERIALIZED VIEW file_log_mv TO table_to_store_data AS
    SELECT id
    FROM file_log
    WHERE id NOT IN (
        SELECT id
        FROM table_to_store_data
        WHERE id IN (
            SELECT id
            FROM file_log
        )
    );
" || exit 1

function count()
{
	COUNT=$(${CLICKHOUSE_CLIENT} --query "select count() from table_to_store_data;")
	echo $COUNT
}

function wait_for_row_count()
{
    local threshold="$1"
    # `FileLog` polls the directory using exponential backoff. With the
    # `poll_directory_watch_events_backoff_max = 1000` setting on the table above
    # the worst-case file-detection latency is bounded at ~1s + scheduling delay.
    # The `no-parallel` tag avoids `BackgroundSchedulePool` contention from
    # concurrent test instances, so this 120s timeout is a generous safety net
    # (matching the timeout in the sibling `02889_file_log_save_errors` test).
    local timeout=120
    local start=$EPOCHSECONDS
    while [[ $(count) -lt threshold ]]; do
        if ((EPOCHSECONDS - start > timeout)); then
            echo "Timeout (${timeout}s) waiting for the minimum number of rows, expected at least ${threshold} row(s). Got $(count)."
            exit 1
        fi
        sleep 0.5
    done
}

wait_for_row_count 1

${CLICKHOUSE_CLIENT} --query "SELECT * FROM table_to_store_data ORDER BY id;"

for i in {1..20}
do
	echo $i >> ${logs_dir}/a.txt
done

wait_for_row_count 11

${CLICKHOUSE_CLIENT} --query "SELECT * FROM table_to_store_data ORDER BY id;"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE file_log;
DROP TABLE table_to_store_data;
DROP TABLE file_log_mv;
"

rm -rf ${logs_dir}
