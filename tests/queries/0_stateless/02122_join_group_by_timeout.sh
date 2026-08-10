#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# No per-test curl --max-time override: rely on the harness ${CLICKHOUSE_CURL}
# (--max-time 120). A cutoff below that could close the connection before the
# server delivers the "Code: 159" timeout error when cancellation is legitimately
# slow (overloaded server / sanitizer builds), dropping that line from the output.
# The query_duration BETWEEN 1 AND 60 assertions below are the real correctness cap.

# Assert a query's duration is within the cancellation window. There is a race
# between the client (HTTP/TCP) response being returned and the terminal
# (non-QueryStart) row being written to the async query_log buffer, so poll with
# SYSTEM FLUSH LOGS until the row appears before asserting. Prints exactly:
#   query_duration\t<0|1>
assert_query_duration() {
    local qid="$1"
    for _ in $(seq 1 60); do
        $CLICKHOUSE_CLIENT -q "system flush logs query_log"
        local cnt
        cnt=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$qid' AND type != 'QueryStart'")
        [ "$cnt" -ge 1 ] && break
        sleep 0.5
    done
    # Bound in ms without rounding: a max_execution_time=1 cancellation can never
    # finish before 1000 ms, so a sub-1s duration is a real regression that
    # round(ms/1000) BETWEEN 1 AND 60 would hide (e.g. 999 rounds to 1).
    $CLICKHOUSE_CLIENT -q "SELECT 'query_duration', query_duration_ms BETWEEN 1000 AND 60000 FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '$qid' AND type != 'QueryStart'"
}

# TCP CLIENT: As of today (02/12/21) uses PullingAsyncPipelineExecutor
### Should be cancelled after 1 second and return a 159 exception (timeout)
### However, in the test, the server can be overloaded, so we assert query duration in the interval of 1 to 60 seconds.
query_id=$(random_str 12)
$CLICKHOUSE_CLIENT --max_result_rows 0 --max_result_bytes 0 --query_id "$query_id" --max_execution_time 1 -q "
    SELECT * FROM
    (
        SELECT a.name as n
        FROM
        (
            SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
        ) AS a,
        (
            SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
        ) as b
        GROUP BY n
    )
    LIMIT 20
    FORMAT Null
" 2>&1 | grep -m1 -o "Code: 159"
assert_query_duration "$query_id"


### Should stop pulling data and return what has been generated already (return code 0)
query_id=$(random_str 12)
$CLICKHOUSE_CLIENT --max_result_rows 0 --max_result_bytes 0 --query_id "$query_id" -q "
    SELECT a.name as n
    FROM
    (
        SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
    ) AS a,
    (
        SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
    ) as b
    FORMAT Null
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'
"
echo $?
assert_query_duration "$query_id"


# HTTP CLIENT: As of today (02/12/21) uses PullingPipelineExecutor
### Should be cancelled after 1 second and return a 159 exception (timeout)
### query_id is set so the HTTP cancellation window is still actively asserted
### (via query_duration below) after dropping the per-test curl --max-time override.
query_id=$(random_str 12)
${CLICKHOUSE_CURL} -q -sS "$CLICKHOUSE_URL&query_id=$query_id&max_execution_time=1&max_result_rows=0&max_result_bytes=0" -d "
    SELECT * FROM
    (
        SELECT a.name as n
        FROM
        (
            SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
        ) AS a,
        (
            SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
        ) as b
        GROUP BY n
    )
    LIMIT 20
    FORMAT Null
" 2>&1 | grep -o "Code: 159" | sort | uniq
assert_query_duration "$query_id"


### Should stop pulling data and return what has been generated already (return code 0)
query_id=$(random_str 12)
${CLICKHOUSE_CURL} -q -sS "$CLICKHOUSE_URL&query_id=$query_id&max_result_rows=0&max_result_bytes=0" -d "
    SELECT a.name as n
          FROM
          (
              SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
          ) AS a,
          (
              SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
          ) as b
          FORMAT Null
          SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'
"
echo $?
assert_query_duration "$query_id"
