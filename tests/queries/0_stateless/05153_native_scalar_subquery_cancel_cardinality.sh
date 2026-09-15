#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test coordinates through the server-global
# `scalar_subquery_before_cardinality_check` PAUSEABLE failpoint; concurrent instances can
# notify or disable each other's channel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

FAILPOINT="scalar_subquery_before_cardinality_check"
CLIENT_PID=""
CLIENT_ERR=""
SCALAR_QUERY="SELECT (SELECT number FROM numbers(2) WHERE sleepEachRow(0.5) = 0)"

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

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"

run_cancelled_scalar()
{
    local analyzer="$1"
    local query_id="scalar_subquery_cardinality_cancel_${CLICKHOUSE_DATABASE}_${analyzer}_${RANDOM}_$$"
    CLIENT_ERR="${CLICKHOUSE_TMP}/native_scalar_subquery_cancel_${analyzer}.err"

    $CLICKHOUSE_CLIENT --query_id "$query_id" \
        --partial_result_on_first_cancel=1 --enable_analyzer="$analyzer" \
        --max_block_size=1 --interactive_delay=1000 --log_queries=1 \
        --log_queries_probability=1 --log_queries_min_query_duration_ms=0 \
        --use_query_cache=1 --query_cache_for_subqueries=1 \
        --query "$SCALAR_QUERY" > /dev/null 2> "$CLIENT_ERR" &
    CLIENT_PID=$!

    if ! timeout 60 $CLICKHOUSE_CLIENT --query \
        "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE" > /dev/null 2>&1; then
        echo "Scalar subquery did not reach the cardinality check: analyzer $analyzer"
        cat "$CLIENT_ERR"
        return 1
    fi

    kill -INT "$CLIENT_PID"
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"
    wait "$CLIENT_PID" || true
    CLIENT_PID=""

    local failed_without_partial_result=0
    for _ in {1..100}; do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
        # The server can observe either `Cancel` first (735) or the second scalar row first (125).
        # Both are valid terminal failures; a successful `QueryFinish` would make this predicate false.
        failed_without_partial_result=$($CLICKHOUSE_CLIENT --query "
            SELECT count() = 1 AND countIf(exception_code IN (735, 125)
                AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')) = 1
            FROM system.query_log WHERE current_database = currentDatabase()
                AND query_id='$query_id' AND type != 'QueryStart'")
        if [[ "$failed_without_partial_result" == 1 ]]; then
            break
        fi
        sleep 0.1
    done
    if [[ "$failed_without_partial_result" != 1 ]]; then
        echo "Scalar subquery unexpectedly returned a partial result: analyzer $analyzer"
        cat "$CLIENT_ERR"
        return 1
    fi

    if $CLICKHOUSE_CLIENT --enable_analyzer="$analyzer" --max_block_size=1 \
        --use_query_cache=1 --query_cache_for_subqueries=1 \
        --query "$SCALAR_QUERY" > /dev/null 2> "$CLIENT_ERR"; then
        echo "Cancelled scalar subquery poisoned the cache: analyzer $analyzer"
        return 1
    fi
    if ! grep -q "INCORRECT_RESULT_OF_SCALAR_SUBQUERY" "$CLIENT_ERR"; then
        echo "Complete scalar subquery did not check cardinality: analyzer $analyzer"
        cat "$CLIENT_ERR"
        return 1
    fi

    rm -f "$CLIENT_ERR"
    CLIENT_ERR=""
    echo "analyzer $analyzer: no partial result, cardinality preserved"
}

run_cancelled_scalar 0
run_cancelled_scalar 1
