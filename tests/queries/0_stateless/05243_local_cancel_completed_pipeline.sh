#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

stderr_file="${CLICKHOUSE_TMP}/05243_local_cancel_completed_pipeline_${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"
local_path=$(mktemp -d --tmpdir="${CLICKHOUSE_TMP}" "05243_local_cancel_completed_pipeline.XXXXXX")
local_pid=""

cleanup()
{
    if [[ -n "$local_pid" ]] && kill -0 "$local_pid" 2>/dev/null
    then
        kill -9 "$local_pid" 2>/dev/null
        wait "$local_pid" 2>/dev/null || true
    fi
    rm -f "$stderr_file"
    rm -rf "$local_path"
}
trap cleanup EXIT

${CLICKHOUSE_LOCAL} --path "$local_path" \
    --query="CREATE TABLE t (x UInt64) ENGINE=MergeTree ORDER BY x" \
    >/dev/null

${CLICKHOUSE_LOCAL} --path "$local_path" --progress=err --interactive_delay=1000 \
    --query="INSERT INTO t SELECT number FROM system.numbers
             SETTINGS max_rows_to_read=0, max_bytes_to_read=0" \
    >/dev/null 2>"$stderr_file" &
local_pid=$!

ready=0
for _ in {0..300}
do
    if grep -q 'Progress:' "$stderr_file"
    then
        ready=1
        break
    fi

    if ! kill -0 "$local_pid" 2>/dev/null
    then
        wait "$local_pid" 2>/dev/null || true
        echo "EXITED BEFORE COMPLETED PIPELINE STARTED"
        cat "$stderr_file"
        exit 1
    fi

    sleep 0.1
done

if [[ "$ready" -ne 1 ]]
then
    echo "NO COMPLETED PIPELINE PROGRESS"
    cat "$stderr_file"
    exit 1
fi

kill -INT "$local_pid" 2>/dev/null

for _ in {0..60}
do
    if ! kill -0 "$local_pid" 2>/dev/null
    then
        exit_code=0
        wait "$local_pid" 2>/dev/null || exit_code=$?
        local_pid=""

        if [[ "$exit_code" -eq 0 ]]
        then
            echo "COMPLETED PIPELINE SUCCEEDED AFTER CANCELLATION"
            cat "$stderr_file"
            exit 1
        fi

        if ! grep -q 'Code: 394' "$stderr_file" || ! grep -q '(QUERY_WAS_CANCELLED)' "$stderr_file"
        then
            echo "WRONG CANCELLATION EXCEPTION"
            cat "$stderr_file"
            exit 1
        fi

        echo "CANCELLED"
        exit 0
    fi
    sleep 0.5
done

echo "HUNG"
exit 1
