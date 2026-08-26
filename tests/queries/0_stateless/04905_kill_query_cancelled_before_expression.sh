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
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT expression_transform_before_expression_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT expression_transform_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT converting_transform_before_expression_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT converting_transform_pause" 2>/dev/null
' EXIT

run_cancelled_query()
{
    local before_failpoint=$1
    local after_failpoint=$2
    local query_id=$3
    local query=$4
    local output_file="${CLICKHOUSE_TMP}/${query_id}.out"

    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${before_failpoint}"

    # The client must outlive the whole orchestration below (waiting for the failpoint, killing the
    # query and waiting for the cancellation to be observed), otherwise `timeout` terminates it
    # before the server reports `QUERY_WAS_CANCELLED` and the test fails spuriously on slow builds.
    timeout 120 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "$query" >"$output_file" 2>&1 &
    local client_pid=$!

    if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${before_failpoint} PAUSE"
    then
        echo "FAIL: timed out waiting for ${before_failpoint}"
        ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}'" >/dev/null
        return 1
    fi

    # The query is deliberately held at the failpoint, so a synchronous `KILL QUERY`
    # waits for it to finish and prevents this test from releasing the failpoint.
    # The stateless-test random settings can enable `http_wait_end_of_query`, which makes the
    # HTTP request wait for the killed query to finish even with `ASYNC`. Override it for the
    # control request so it can release the failpoint immediately after dispatching the kill.
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '${query_id}' ASYNC" >/dev/null

    # Do not release the failpoint until the asynchronous kill has reached the query. Otherwise,
    # the query can resume and park at the post-expression failpoint before cancellation is set.
    # Poll over HTTP: starting `clickhouse-client` hundreds of times costs more wall-clock time
    # than the query client is allowed to live on a debug or sanitizer build.
    local cancelled=0
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline ))
    do
        cancelled=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND is_cancelled")
        [[ "$cancelled" -ge 1 ]] && break
        sleep 0.1
    done
    [[ "$cancelled" -ge 1 ]] || { echo "FAIL: the query was not marked as cancelled in system.processes"; ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"; return 1; }

    # Arm the post-expression failpoint only after the kill is confirmed. These failpoints are
    # global: while one is armed but not yet consumed, the first unrelated query passing the same
    # transform gets caught by it. In particular, `KILL QUERY` runs an internal `SELECT` over
    # `system.processes`, and every `SELECT` pipeline contains an `ExpressionTransform` — arming
    # `expression_transform_pause` before the kill deadlocks the kill itself.
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${after_failpoint}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"

    wait "$client_pid"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${after_failpoint}"

    grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: the client did not report QUERY_WAS_CANCELLED"; cat "$output_file"; return 1; }
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

run_cancelled_query \
    expression_transform_before_expression_pause \
    expression_transform_pause \
    "kill_query_expression_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "SELECT sipHash64(number) FROM numbers(1000000) FORMAT Null SETTINGS max_threads = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_converting_before_expression"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE kill_query_converting_before_expression (x UInt64, y UInt64 DEFAULT sipHash64(x)) ENGINE = Memory"

run_cancelled_query \
    converting_transform_before_expression_pause \
    converting_transform_pause \
    "kill_query_converting_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "INSERT INTO kill_query_converting_before_expression (x) SELECT number FROM numbers(1000000) SETTINGS max_threads = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE kill_query_converting_before_expression"

echo "OK"
