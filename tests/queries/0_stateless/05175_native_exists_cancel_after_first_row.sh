#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: this test coordinates through the server-global
# `scalar_subquery_before_cardinality_check` PAUSEABLE failpoint; concurrent instances can
# notify or disable each other's channel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=helpers/native_cancel.sh
. "$CUR_DIR"/helpers/native_cancel.sh

set -euo pipefail

FAILPOINT="scalar_subquery_before_cardinality_check"
CLIENT_PID=""
CLIENT_OUT=""
CLIENT_ERR=""

cleanup()
{
    native_cancel_cleanup_client
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    rm -f "$CLIENT_OUT" "$CLIENT_ERR"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"

query_id="exists_subquery_partial_cancel_${CLICKHOUSE_DATABASE}_${RANDOM}_$$"
CLIENT_OUT="${CLICKHOUSE_TMP}/native_exists_cancel.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/native_exists_cancel.err"

$CLICKHOUSE_CLIENT --query_id "$query_id" \
    --partial_result_on_first_cancel=1 --enable_analyzer=1 \
    --execute_exists_as_scalar_subquery=1 --interactive_delay=1000 \
    --max_block_size=1 --use_query_cache=0 \
    --log_queries=1 --log_queries_probability=1 --log_queries_min_query_duration_ms=0 \
    --query "SELECT EXISTS(SELECT number FROM numbers(2) WHERE sleepEachRow(0.5) = 0)" \
    > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
CLIENT_PID=$!

if ! native_cancel_wait_for_failpoint \
    "$FAILPOINT" \
    "EXISTS subquery did not produce its first row" \
    "$CLIENT_ERR"; then
    exit 1
fi

kill -INT "$CLIENT_PID"
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FAILPOINT"
wait "$CLIENT_PID"
CLIENT_PID=""

if [[ "$(tr -d '[:space:]' < "$CLIENT_OUT")" != "1" ]]; then
    echo "EXISTS did not finish with the determined result"
    cat "$CLIENT_OUT"
    cat "$CLIENT_ERR"
    exit 1
fi

if ! native_cancel_wait_for_query_log "$query_id" \
    "count() = 1 AND countIf(type = 'QueryFinish' AND exception_code = 0) = 1"; then
    echo "EXISTS partial cancellation did not finish successfully"
    cat "$CLIENT_ERR"
    exit 1
fi

echo "exists: 1, partial result"
