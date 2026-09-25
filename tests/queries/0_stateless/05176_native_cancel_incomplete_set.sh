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

run_cancelled_set()
{
    local operator="$1"
    local label="$2"
    local query_id="incomplete_set_cancel_${CLICKHOUSE_DATABASE}_${label}_${RANDOM}_$$"
    CLIENT_ERR="${CLICKHOUSE_TMP}/native_cancel_incomplete_set_${label}.err"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"

    $CLICKHOUSE_CLIENT --query_id "$query_id" \
        --partial_result_on_first_cancel=1 \
        --use_index_for_in_with_subqueries=0 --max_block_size=1 \
        --interactive_delay=1000 --use_query_cache=0 \
        --log_queries=1 --log_queries_probability=1 --log_queries_min_query_duration_ms=0 \
        --query "
            SELECT count() FROM numbers(3) WHERE number $operator (
                SELECT number FROM numbers(1000000) WHERE sleepEachRow(0.001) = 0
            ) FORMAT Null" > /dev/null 2> "$CLIENT_ERR" &
    CLIENT_PID=$!

    if ! native_cancel_wait_for_failpoint \
        "$FAILPOINT" \
        "Set builder did not consume its first chunk: $operator" \
        "$CLIENT_ERR"; then
        return 1
    fi

    kill -INT "$CLIENT_PID"
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"

    if wait "$CLIENT_PID"; then
        CLIENT_PID=""
        echo "Incomplete $operator set was accepted"
        return 1
    fi
    CLIENT_PID=""

    if ! grep -q "QUERY_WAS_CANCELLED" "$CLIENT_ERR"; then
        echo "Incomplete $operator set produced an unexpected error"
        cat "$CLIENT_ERR"
        return 1
    fi

    if ! native_cancel_wait_for_query_log "$query_id" \
        "count() = 1
            AND countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 394) = 1
            AND countIf(type = 'QueryFinish') = 0"; then
        echo "Incomplete $operator set reached a successful terminal state"
        cat "$CLIENT_ERR"
        return 1
    fi

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
    rm -f "$CLIENT_ERR"
    CLIENT_ERR=""
    echo "$operator: incomplete set rejected"
}

run_cancelled_set IN in
run_cancelled_set "NOT IN" not_in
