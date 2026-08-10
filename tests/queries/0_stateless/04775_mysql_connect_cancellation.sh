#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification:
#   no-fasttest: depends on libmysql (MySQL table function), not built in fast test.
# No no-parallel tag: the engine table lives in ${CLICKHOUSE_DATABASE} and inspecting it does not
# connect, so concurrent copies cannot observe each other (unlike 04507, which ATTACHes a MySQL
# database).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query cancelled while connecting to MySQL must stop promptly. The connect blocks in poll()
# inside the client library during query analysis (schema inference), where no pipeline exists,
# so neither KILL QUERY nor max_execution_time took effect until the connect timed out once per
# retry: connection_max_tries * connect_timeout.
# 192.0.2.1 (RFC 5737 TEST-NET-1) drops SYN packets rather than refusing them, so the connect
# really blocks for the whole timeout; against a refused host it returns immediately and the
# test would pass with or without the fix.
MYSQL_ARGS="'192.0.2.1:3306', 'db', 'tbl', 'u', 'p'"

CLICKHOUSE_CLIENT_QUIET=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=fatal/g")

# Durations are checked against 20 s: cancellation completes in ~2 s once fixed, while the
# unfixed server took 100 s (one try), 300 s (three tries) and 133 s (unbounded connect), so
# the bound separates them with a wide margin on slow CI.
LIMIT_MS=20000

report()
{
    local name=$1 elapsed_ms=$2 expected=$3 error=$4
    local verdict="ok"
    [ "${elapsed_ms}" -lt "${LIMIT_MS}" ] || verdict="TOO SLOW (${elapsed_ms}ms)"
    echo "${error}" | grep -q "${expected}" || verdict="wrong error: ${error}"
    echo "${name} ${verdict}"
}

# Run a query in the background and cancel it once it is really inside the MySQL connect.
# Echoes the elapsed milliseconds; the error text is left in ${CLICKHOUSE_TMP}/${name}.err.
# Returns non-zero if the query never started, in which case the caller must not judge it.
run_and_cancel()
{
    local name=$1 query=$2
    local query_id="${CLICKHOUSE_DATABASE}_${name}"
    local start_ms end_ms

    start_ms=$(date +%s%3N)
    ${CLICKHOUSE_CLIENT_QUIET} --query_id "${query_id}" -q "${query}" \
        > "${CLICKHOUSE_TMP}/${name}.err" 2>&1 &
    local client_pid=$!

    # Cancel only once the query is actually running, so the arm cannot pass by cancelling
    # something that never started.
    local seen=0
    for _ in {1..200}; do
        if [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${query_id}'")" = "1" ]; then
            seen=1
            break
        fi
        sleep 0.1
    done
    if [ "${seen}" = "0" ]; then
        wait "${client_pid}" 2>/dev/null
        return 1
    fi

    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${query_id}' SYNC FORMAT Null"
    wait "${client_pid}" 2>/dev/null
    end_ms=$(date +%s%3N)

    echo "$((end_ms - start_ms))"
}

# Cancel a query that is inside the MySQL connect, and report how long it took to come back.
kill_arm()
{
    local name=$1 settings=$2
    local elapsed_ms error

    if ! elapsed_ms=$(run_and_cancel "${name}" "SELECT * FROM mysql(${MYSQL_ARGS}, SETTINGS ${settings})"); then
        echo "${name} query never appeared in system.processes"
        return
    fi

    error=$(cat "${CLICKHOUSE_TMP}/${name}.err")
    rm -f "${CLICKHOUSE_TMP}/${name}.err"
    report "${name}" "${elapsed_ms}" "QUERY_WAS_CANCELLED" "${error}"
}

# One try: the check before the final error is the only cancellation checkpoint left, so this
# arm pins the error identity. Without it the cancellation surfaces as ALL_CONNECTION_TRIES_FAILED.
kill_arm kill_one_try "connect_timeout = 100, connection_max_tries = 1"
# Three tries: latency alone does not pin the per-attempt check, because the sliced wait already
# bounds each attempt to a fraction of a second. kill_retry_count below is what pins it.
kill_arm kill_three_tries "connect_timeout = 100, connection_max_tries = 3"
# connect_timeout = 0 means wait indefinitely, which must stay killable and must not fail early.
kill_arm kill_unbounded_timeout "connect_timeout = 0, connection_max_tries = 1"

# The per-attempt check must stop the retry loop, not merely shorten each attempt. The pool logs
# one line per completed retry, so count them: attempt 1 is already under way when the
# cancellation arrives and legitimately logs its line, and retries 2 and 3 must not happen.
# Expect exactly 1, never 0.
KILL_RETRY_NAME=kill_retry_count
if ELAPSED_MS=$(run_and_cancel "${KILL_RETRY_NAME}" \
        "SELECT * FROM mysql(${MYSQL_ARGS}, SETTINGS connect_timeout = 100, connection_max_tries = 3)"); then
    rm -f "${CLICKHOUSE_TMP}/${KILL_RETRY_NAME}.err"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
    # system.text_log is MergeTree backed, so the randomized parallel replicas settings apply to
    # reads of it. Pin them off per query rather than tagging the whole test.
    RETRIES=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.text_log
        WHERE query_id = '${CLICKHOUSE_DATABASE}_${KILL_RETRY_NAME}'
          AND message LIKE 'Connection to mysql failed%times'
        SETTINGS enable_parallel_replicas = 0")
    if [ "${RETRIES}" = "1" ]; then
        echo "${KILL_RETRY_NAME} ok"
    else
        echo "${KILL_RETRY_NAME} wrong retries: ${RETRIES}"
    fi
else
    echo "${KILL_RETRY_NAME} query never appeared in system.processes"
fi

# A MySQL engine table connects from inside the pipeline (MySQLWithFailoverSource) instead of
# during analysis, which is a second entry into the same pool. Explicit columns keep CREATE from
# connecting, so the connect happens on the SELECT.
ENGINE_NAME=kill_engine_table
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.mysql_engine_tbl (x Int32)
    ENGINE = MySQL(${MYSQL_ARGS}) SETTINGS connect_timeout = 100, connection_max_tries = 1"
if ELAPSED_MS=$(run_and_cancel "${ENGINE_NAME}" "SELECT * FROM ${CLICKHOUSE_DATABASE}.mysql_engine_tbl"); then
    ERROR=$(cat "${CLICKHOUSE_TMP}/${ENGINE_NAME}.err")
    rm -f "${CLICKHOUSE_TMP}/${ENGINE_NAME}.err"
    # Prove the arm really went through the storage rather than repeating the table function:
    # only the engine path logs under the StorageMySQL logger.
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
    VIA_STORAGE=$(${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.text_log
        WHERE query_id = '${CLICKHOUSE_DATABASE}_${ENGINE_NAME}' AND logger_name LIKE 'StorageMySQL%'
        SETTINGS enable_parallel_replicas = 0")
    if [ "${VIA_STORAGE}" = "1" ]; then
        report "${ENGINE_NAME}" "${ELAPSED_MS}" "QUERY_WAS_CANCELLED" "${ERROR}"
    else
        echo "${ENGINE_NAME} did not reach the MySQL engine path"
    fi
else
    echo "${ENGINE_NAME} query never appeared in system.processes"
fi
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.mysql_engine_tbl"

# max_execution_time is enforced by a background watchdog that only raises the cancellation
# flag, so it reaches the connect through the same checks as KILL QUERY.
START_MS=$(date +%s%3N)
ERROR=$(${CLICKHOUSE_CLIENT_QUIET} -q \
    "SELECT * FROM mysql(${MYSQL_ARGS}, SETTINGS connect_timeout = 100, connection_max_tries = 1)
     SETTINGS max_execution_time = 5" 2>&1)
END_MS=$(date +%s%3N)
report max_execution_time "$((END_MS - START_MS))" "TIMEOUT_EXCEEDED" "${ERROR}"

# Without cancellation nothing changes: the same error, and the connect still consumes its
# timeout instead of failing early.
START_MS=$(date +%s%3N)
ERROR=$(${CLICKHOUSE_CLIENT_QUIET} -q \
    "SELECT * FROM mysql(${MYSQL_ARGS}, SETTINGS connect_timeout = 3, connection_max_tries = 1)" 2>&1)
END_MS=$(date +%s%3N)
ELAPSED_MS=$((END_MS - START_MS))
if [ "${ELAPSED_MS}" -ge 2000 ]; then
    report not_cancelled_still_waits "${ELAPSED_MS}" "ALL_CONNECTION_TRIES_FAILED" "${ERROR}"
else
    echo "not_cancelled_still_waits returned too early (${ELAPSED_MS}ms), connect did not block"
fi
