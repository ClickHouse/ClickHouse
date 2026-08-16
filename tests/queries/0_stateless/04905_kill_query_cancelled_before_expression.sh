#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan

# Verify that cancellation between task dispatch and expression execution skips the expression.
# Each post-expression failpoint stays enabled: without the pre-execution cancellation guard, the
# query reaches it after the kill and times out instead of reporting `QUERY_WAS_CANCELLED`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap '
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_before_expression_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_before_expression_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_pause" 2>/dev/null
' EXIT

run_cancelled_query()
{
    local before_failpoint=$1
    local after_failpoint=$2
    local query_id=$3
    local query=$4
    local output_file="${CLICKHOUSE_TMP}/${query_id}.out"

    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${before_failpoint}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${after_failpoint}"

    timeout 30 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "$query" >"$output_file" 2>&1 &
    local client_pid=$!

    if ! timeout 30 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${before_failpoint} PAUSE"
    then
        echo "FAIL: timed out waiting for ${before_failpoint}"
        ${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '${query_id}'" >/dev/null
        return 1
    fi

    # The query is deliberately held at the failpoint, so a synchronous `KILL QUERY`
    # waits for it to finish and prevents this test from releasing the failpoint.
    # The stateless-test random settings can enable `http_wait_end_of_query`, which makes the
    # HTTP request wait for the killed query to finish even with `ASYNC`. Override it for the
    # control request so it can release the failpoint immediately after dispatching the kill.
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}' ASYNC" >/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"

    wait "$client_pid"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${after_failpoint}"

    grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; return 1; }
}

run_cancelled_query \
    filter_transform_before_expression_pause \
    filter_transform_pause \
    "kill_query_filter_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "SELECT count() FROM numbers(1000000) WHERE sipHash64(number) % 2 = 1 FORMAT Null SETTINGS max_threads = 1"

run_cancelled_query \
    totals_having_transform_before_expression_pause \
    totals_having_transform_pause \
    "kill_query_having_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "SELECT number % 10 AS k, count() FROM numbers(1000000) GROUP BY k WITH TOTALS HAVING sipHash64(count()) % 2 >= 0 FORMAT Null SETTINGS max_threads = 1"

echo "OK"
