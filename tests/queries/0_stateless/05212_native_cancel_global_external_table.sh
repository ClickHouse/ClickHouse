#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test coordinates through the server-global
# `creating_sets_transform_after_first_chunk` PAUSEABLE_ONCE failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=helpers/native_cancel.sh
. "$CUR_DIR"/helpers/native_cancel.sh

set -euo pipefail

FAILPOINT="creating_sets_transform_after_first_chunk"
CLIENT_PID=""
CLIENT_ERR=""

cleanup()
{
    native_cancel_cleanup_client
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    if [[ -n "$CLIENT_ERR" ]]; then
        rm -f "$CLIENT_ERR"
    fi
}
trap cleanup EXIT

run_cancelled_global_subquery()
{
    local label="$1"
    local query="$2"
    local control_query="$3"
    local query_id="global_external_table_cancel_${CLICKHOUSE_DATABASE}_${label}_${RANDOM}_$$"
    CLIENT_ERR="${CLICKHOUSE_TMP}/native_cancel_global_external_table_${label}.err"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"

    $CLICKHOUSE_CLIENT --query_id "$query_id" \
        --partial_result_on_first_cancel=1 --enable_analyzer=1 \
        --max_threads=1 --max_block_size=1 --interactive_delay=1000 --use_query_cache=0 \
        --min_external_table_block_size_rows=1 --min_external_table_block_size_bytes=0 \
        --log_queries=1 --log_queries_probability=1 --log_queries_min_query_duration_ms=0 \
        --query "$query" > /dev/null 2> "$CLIENT_ERR" &
    CLIENT_PID=$!

    if ! native_cancel_wait_for_failpoint \
        "$FAILPOINT" \
        "$label did not start filling the external table" \
        "$CLIENT_ERR"; then
        return 1
    fi

    kill -INT "$CLIENT_PID"
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"

    if wait "$CLIENT_PID"; then
        CLIENT_PID=""
        echo "$label accepted an incomplete external table"
        return 1
    fi
    CLIENT_PID=""

    if ! grep -q "QUERY_WAS_CANCELLED" "$CLIENT_ERR"; then
        echo "$label produced an unexpected error"
        cat "$CLIENT_ERR"
        return 1
    fi

    if ! native_cancel_wait_for_query_log "$query_id" \
        "count() = 1
            AND countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 394) = 1
            AND countIf(type = 'QueryFinish') = 0"; then
        echo "$label reached an unexpected terminal state"
        cat "$CLIENT_ERR"
        return 1
    fi

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
    rm -f "$CLIENT_ERR"
    CLIENT_ERR=""

    echo "$label: incomplete external table rejected"
    $CLICKHOUSE_CLIENT --enable_analyzer=1 --query "$control_query"
}

SLOW_RHS="SELECT number FROM numbers(1000000) WHERE sleepEachRow(0.001) = 0"

run_cancelled_global_subquery "GLOBAL IN" "
    SELECT count() FROM remote('127.0.0.{1,2}', numbers(3))
    WHERE number GLOBAL IN ($SLOW_RHS) FORMAT Null" "
    SELECT count() = 6 FROM remote('127.0.0.{1,2}', numbers(3))
    WHERE number GLOBAL IN (SELECT number FROM numbers(3))"

run_cancelled_global_subquery "GLOBAL JOIN" "
    SELECT count() FROM remote('127.0.0.{1,2}', numbers(3)) AS lhs
    GLOBAL ANY INNER JOIN ($SLOW_RHS) AS rhs USING (number) FORMAT Null" "
    SELECT count() = 6 FROM remote('127.0.0.{1,2}', numbers(3)) AS lhs
    GLOBAL ANY INNER JOIN (SELECT number FROM numbers(3)) AS rhs USING (number)"
