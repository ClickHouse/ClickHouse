#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification:
#   no-fasttest: depends on libmysql (MySQL table function), not built in fast test.
# No no-parallel tag: the mysql() table function attaches nothing to system.tables, so
# concurrent copies cannot observe each other (unlike 04507, which ATTACHes a MySQL database).

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

# Cancel a query that is inside the MySQL connect, and report how long it took to come back.
kill_arm()
{
    local name=$1 settings=$2
    local query_id="${CLICKHOUSE_DATABASE}_${name}"
    local start_ms end_ms error

    start_ms=$(date +%s%3N)
    ${CLICKHOUSE_CLIENT_QUIET} --query_id "${query_id}" \
        -q "SELECT * FROM mysql(${MYSQL_ARGS}, SETTINGS ${settings})" > "${CLICKHOUSE_TMP}/${name}.err" 2>&1 &
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
        echo "${name} query never appeared in system.processes"
        wait "${client_pid}" 2>/dev/null
        return
    fi

    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${query_id}' SYNC FORMAT Null"
    wait "${client_pid}" 2>/dev/null
    end_ms=$(date +%s%3N)

    error=$(cat "${CLICKHOUSE_TMP}/${name}.err")
    rm -f "${CLICKHOUSE_TMP}/${name}.err"
    report "${name}" "$((end_ms - start_ms))" "QUERY_WAS_CANCELLED" "${error}"
}

# One try: the check before the final error is the only cancellation checkpoint left, so this
# arm pins the error identity. Without it the cancellation surfaces as ALL_CONNECTION_TRIES_FAILED.
kill_arm kill_one_try "connect_timeout = 100, connection_max_tries = 1"
# Three tries: the per-attempt check stops the remaining attempts, so this arm pins the latency.
kill_arm kill_three_tries "connect_timeout = 100, connection_max_tries = 3"
# connect_timeout = 0 means wait indefinitely, which must stay killable and must not fail early.
kill_arm kill_unbounded_timeout "connect_timeout = 0, connection_max_tries = 1"

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
