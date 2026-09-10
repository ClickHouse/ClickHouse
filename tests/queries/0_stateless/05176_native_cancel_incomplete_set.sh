#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test coordinates through the server-global
# `creating_sets_transform_after_first_chunk` PAUSEABLE_ONCE failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

FAILPOINT="creating_sets_transform_after_first_chunk"
CLIENT_PID=""
CLIENT_ERR=""

cleanup()
{
    if [[ -n "$CLIENT_PID" ]]; then
        kill -KILL "$CLIENT_PID" 2>/dev/null || true
        wait "$CLIENT_PID" 2>/dev/null || true
    fi
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    if [[ -n "$CLIENT_ERR" ]]; then
        rm -f "$CLIENT_ERR"
    fi
}
trap cleanup EXIT

run_cancelled_set()
{
    local analyzer="$1"
    local operator="$2"
    local label="$3"
    local query_id="incomplete_set_cancel_${CLICKHOUSE_DATABASE}_${analyzer}_${label}_${RANDOM}_$$"
    CLIENT_ERR="${CLICKHOUSE_TMP}/native_cancel_incomplete_set_${analyzer}_${label}.err"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"

    $CLICKHOUSE_CLIENT --query_id "$query_id" \
        --partial_result_on_first_cancel=1 --enable_analyzer="$analyzer" \
        --use_index_for_in_with_subqueries=0 --max_block_size=1 \
        --interactive_delay=1000 --use_query_cache=0 \
        --log_queries=1 --log_queries_probability=1 --log_queries_min_query_duration_ms=0 \
        --query "
            SELECT count() FROM numbers(3) WHERE number $operator (
                SELECT number FROM numbers(1000000) WHERE sleepEachRow(0.001) = 0
            ) FORMAT Null" > /dev/null 2> "$CLIENT_ERR" &
    CLIENT_PID=$!

    if ! timeout 60 $CLICKHOUSE_CLIENT --query \
        "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE" > /dev/null 2>&1; then
        echo "Set builder did not consume its first chunk: analyzer $analyzer, $operator"
        cat "$CLIENT_ERR"
        return 1
    fi

    kill -INT "$CLIENT_PID"
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"

    if wait "$CLIENT_PID"; then
        CLIENT_PID=""
        echo "Incomplete $operator set was accepted: analyzer $analyzer"
        return 1
    fi
    CLIENT_PID=""

    if ! grep -q "QUERY_WAS_CANCELLED" "$CLIENT_ERR"; then
        echo "Incomplete $operator set produced an unexpected error: analyzer $analyzer"
        cat "$CLIENT_ERR"
        return 1
    fi

    local rejected_without_finish=0
    for _ in {1..100}; do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
        rejected_without_finish=$($CLICKHOUSE_CLIENT --query "
            SELECT count() = 1
                AND countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 394) = 1
                AND countIf(type = 'QueryFinish') = 0
            FROM system.query_log WHERE current_database = currentDatabase()
                AND query_id = '$query_id' AND type != 'QueryStart'")
        if [[ "$rejected_without_finish" == 1 ]]; then
            break
        fi
        sleep 0.1
    done

    if [[ "$rejected_without_finish" != 1 ]]; then
        echo "Incomplete $operator set reached a successful terminal state: analyzer $analyzer"
        cat "$CLIENT_ERR"
        return 1
    fi

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
    rm -f "$CLIENT_ERR"
    CLIENT_ERR=""
    echo "analyzer $analyzer, $operator: incomplete set rejected"
}

run_cancelled_set 0 IN in
run_cancelled_set 0 "NOT IN" not_in
run_cancelled_set 1 IN in
run_cancelled_set 1 "NOT IN" not_in
