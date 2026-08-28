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
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT check_constraints_transform_before_expression_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT check_constraints_transform_pause" 2>/dev/null
' EXIT

# Poll over HTTP: starting `clickhouse-client` hundreds of times costs more wall-clock time
# than the query client is allowed to live on a debug or sanitizer build.
scalar()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

# The target query is deliberately held at a failpoint, so a synchronous `KILL QUERY` waits for it
# to finish and prevents this test from releasing the failpoint. The stateless-test random settings
# can enable `http_wait_end_of_query`, which makes the HTTP request wait for the killed query to
# finish even with `ASYNC`. Override it for the control request so it returns immediately.
kill_query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$1' ASYNC" >/dev/null
}

# Rows read by the target query so far, or `0` once it is gone from `system.processes`.
read_rows_of()
{
    scalar "SELECT sum(read_rows) FROM system.processes WHERE query_id = '$1'"
}

# Kill the query paused at `$before_failpoint` and check that it returns without evaluating the
# expression: `$after_failpoint` is armed only after the cancellation has been observed, so a query
# that still evaluated the expression would be caught by it instead of reporting the cancellation.
finish_cancelled_query()
{
    local before_failpoint=$1
    local after_failpoint=$2
    local query_id=$3
    local client_pid=$4
    local output_file=$5

    kill_query "$query_id"

    # Do not release the failpoint until the asynchronous kill has reached the query. Otherwise,
    # the query can resume and park at the post-expression failpoint before cancellation is set.
    local cancelled=0
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline ))
    do
        cancelled=$(scalar "SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND is_cancelled")
        [[ "$cancelled" -ge 1 ]] && break
        sleep 0.1
    done
    if [[ "$cancelled" -lt 1 ]]
    then
        echo "FAIL: ${before_failpoint}: the query was not marked as cancelled in system.processes"
        ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"
        return 1
    fi

    # Arm the post-expression failpoint only after the kill is confirmed. These failpoints are
    # global: while one is armed but not yet consumed, the first unrelated query passing the same
    # transform gets caught by it. In particular, `KILL QUERY` runs an internal `SELECT` over
    # `system.processes`, and every `SELECT` pipeline contains an `ExpressionTransform` — arming
    # `expression_transform_pause` before the kill deadlocks the kill itself.
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${after_failpoint}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"

    wait "$client_pid"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${after_failpoint}"

    grep -qF "QUERY_WAS_CANCELLED" "$output_file" \
        || { echo "FAIL: ${before_failpoint}: the client did not report QUERY_WAS_CANCELLED"; cat "$output_file"; return 1; }
}

# `FilterTransform`, `TotalsHavingTransform` and `CheckConstraintsTransform` appear only in a query
# of a specific shape, so their failpoints can be armed before the query starts: no unrelated query
# can reach them.
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
        kill_query "$query_id"
        return 1
    fi

    finish_cancelled_query "$before_failpoint" "$after_failpoint" "$query_id" "$client_pid" "$output_file"
}

# `ExpressionTransform` and `ConvertingTransform` are on the pipeline of *every* query, including
# the background `INSERT`s that flush the system logs. A one-shot pause armed before the query
# starts is therefore consumed by an unrelated query almost every time. Instead, run a query that
# keeps feeding chunks into the transform, arm the failpoint while it runs, and check that the
# paused thread belongs to this query: the transform holds the whole single-threaded pipeline, so
# the query stops reading rows. If an unrelated query took the pause, release it and arm again.
run_cancelled_streaming_query()
{
    local before_failpoint=$1
    local after_failpoint=$2
    local query_id=$3
    local query=$4
    local output_file="${CLICKHOUSE_TMP}/${query_id}.out"

    timeout 120 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "$query" >"$output_file" 2>&1 &
    local client_pid=$!

    local read_rows=0
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline ))
    do
        read_rows=$(read_rows_of "$query_id")
        [[ "$read_rows" -gt 0 ]] && break
        sleep 0.1
    done
    if [[ "$read_rows" -le 0 ]]
    then
        echo "FAIL: ${before_failpoint}: the query did not start reading rows"
        kill_query "$query_id"
        return 1
    fi

    local attempt
    for attempt in {1..8}
    do
        ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${before_failpoint}"

        if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${before_failpoint} PAUSE"
        then
            echo "FAIL: timed out waiting for ${before_failpoint}"
            ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"
            kill_query "$query_id"
            return 1
        fi

        local rows_before
        local rows_after
        rows_before=$(read_rows_of "$query_id")
        sleep 1
        rows_after=$(read_rows_of "$query_id")

        if [[ "$rows_before" -le 0 || "$rows_after" -le 0 ]]
        then
            echo "FAIL: ${before_failpoint}: the query finished before the failpoint caught it"
            ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"
            return 1
        fi

        if [[ "$rows_before" == "$rows_after" ]]
        then
            finish_cancelled_query "$before_failpoint" "$after_failpoint" "$query_id" "$client_pid" "$output_file"
            return
        fi

        # An unrelated query consumed the pause: release it and arm the failpoint again.
        ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${before_failpoint}"
    done

    echo "FAIL: ${before_failpoint}: unrelated queries kept consuming the failpoint"
    kill_query "$query_id"
    return 1
}

run_cancelled_query \
    filter_transform_before_expression_pause \
    filter_transform_pause \
    "kill_query_filter_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "SELECT count() FROM numbers(1000000) WHERE sipHash64(number) % 2 = 1 FORMAT Null SETTINGS max_threads = 1" || exit 1

run_cancelled_query \
    totals_having_transform_before_expression_pause \
    totals_having_transform_pause \
    "kill_query_having_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "SELECT number % 10 AS k, count() FROM numbers(1000000) GROUP BY k WITH TOTALS HAVING sipHash64(count()) % 2 >= 0 FORMAT Null SETTINGS max_threads = 1" || exit 1

run_cancelled_streaming_query \
    expression_transform_before_expression_pause \
    expression_transform_pause \
    "kill_query_expression_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "SELECT sipHash64(number) FROM numbers(1000000000) FORMAT Null SETTINGS max_threads = 1, max_block_size = 8192" || exit 1

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_converting_before_expression"
# `Null` keeps the table from growing while the query streams: only the conversion of the omitted
# `y` column, which `ConvertingTransform` performs, matters here.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE kill_query_converting_before_expression (x UInt64, y UInt64 DEFAULT sipHash64(x)) ENGINE = Null"

run_cancelled_streaming_query \
    converting_transform_before_expression_pause \
    converting_transform_pause \
    "kill_query_converting_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "INSERT INTO kill_query_converting_before_expression (x) SELECT number FROM numbers(1000000000) SETTINGS max_threads = 1, max_block_size = 8192, min_insert_block_size_rows = 8192, min_insert_block_size_bytes = 0" || exit 1

${CLICKHOUSE_CLIENT} -q "DROP TABLE kill_query_converting_before_expression"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_check_constraints_before_expression"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE kill_query_check_constraints_before_expression (x UInt64, CONSTRAINT c CHECK sipHash64(x) % 2 >= 0) ENGINE = Null"

run_cancelled_query \
    check_constraints_transform_before_expression_pause \
    check_constraints_transform_pause \
    "kill_query_check_constraints_before_expression_${CLICKHOUSE_DATABASE}_$RANDOM" \
    "INSERT INTO kill_query_check_constraints_before_expression SELECT number FROM numbers(1000000) SETTINGS max_threads = 1" || exit 1

${CLICKHOUSE_CLIENT} -q "DROP TABLE kill_query_check_constraints_before_expression"

echo "OK"
