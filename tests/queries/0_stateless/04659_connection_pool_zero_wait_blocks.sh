#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the pool is keyed on host, port, credentials and pool size, none of which a test
# database makes unique, so concurrent copies queue behind one connection. Measured with 6 copies:
# 7s on its own, 16s together.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Two queries share a remote connection pool of size 1, so the second one has to wait for the first.
# With connection_pool_max_wait_ms = 0 the waiter must block on the pool instead of retrying with a
# zero length wait, which is what the "Waiting 0 ms." line below would report.

# The suffix and the per-call start time below keep one run's log rows out of another's. A test
# database is not enough on its own: the stress threads pass a fixed --database, so rows from an
# earlier run could satisfy the lower bound without this run ever waiting, and could also push the
# upper bound over its limit.
QUERY_PREFIX="04659_${CLICKHOUSE_DATABASE}_$(random_str 8)"

function running() {
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id IN (${1})"
}

# Runs one pair of queries at the given connection_pool_max_wait_ms and leaves the log rows the
# assertion below reads. The holder does not finish on its own: it is killed once the waiter is
# seen contending, so the pool is always full while the waiter reaches it and no attempt has to be
# repeated to land that overlap.
function contend() {
    local wait_ms=$1 label=$2
    local holder="${QUERY_PREFIX}_${label}_holder" waiter="${QUERY_PREFIX}_${label}_waiter"
    local holder_pid waiter_pid contended=0

    # Read the clock from the server the log rows come from, so the predicate needs no clock
    # alignment.
    CONTEND_START=$(${CLICKHOUSE_CLIENT} --query "SELECT toString(now64(6))")

    # Sleeps far longer than the handshake below needs, so the connection stays held until the kill
    # frees it. The per block sleep cap is what bounds the row count.
    timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${holder}" --query "
        SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', numbers(30)) WHERE sleepEachRow(1)
        SETTINGS prefer_localhost_replica = 0, distributed_connections_pool_size = 1,
                 connection_pool_max_wait_ms = ${wait_ms}, function_sleep_max_microseconds_per_block = 60000000
    " < /dev/null > /dev/null 2>&1 &
    holder_pid=$!

    # Only the holder can free the connection, so the waiter starts once the holder owns it. The
    # bound stops the loop from hanging if the holder failed outright, in which case its wait below
    # reports it.
    for _ in {1..600}; do
        [[ $(running "'${holder}'") != 0 ]] && break
        sleep 0.1
    done

    timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${waiter}" --query "
        SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', numbers(1)) WHERE sleepEachRow(1)
        SETTINGS prefer_localhost_replica = 0, distributed_connections_pool_size = 1,
                 connection_pool_max_wait_ms = ${wait_ms}, function_sleep_max_microseconds_per_block = 60000000
    " < /dev/null > /dev/null 2>&1 &
    waiter_pid=$!

    # Both queries in flight together is what leaves the waiter on a full pool.
    for _ in {1..600}; do
        [[ $(running "'${holder}', '${waiter}'") == 2 ]] && { contended=1; break; }
        sleep 0.05
    done

    # Being in system.processes only means the waiter has started, so give it time to reach the pool
    # and log. On the finite wait this is also the window its retries fall in, which is what the
    # frequency limiter has to throttle.
    sleep 2

    # Bounded and asynchronous because on the failing arm the zero length wait starves the whole
    # server: the holder does not return, and neither does a kill that waits for it. The bound is
    # what lets that arm still reach the assertion below instead of running into the test timeout.
    # shellcheck disable=SC2086
    timeout 30 ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${holder}' ASYNC" > /dev/null

    # The killed holder is expected to fail, but the waiter has to be handed the connection the kill
    # released and run to completion. Without this a waiter that logged the line and then blocked
    # for good, because it was never woken, would still satisfy the log assertions below.
    wait "$holder_pid" || true
    wait "$waiter_pid" || echo "the waiter did not get the connection back"

    [[ ${contended} == 1 ]] || echo "the queries never ran at the same time, so the pool was never full"
}

contend 0 zero

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"

# The waiter has to reach the pool once, and then stay there: a wait that returned immediately and
# re-entered the loop would report the same line many times over (measured in the thousands when the
# blocking wait is replaced by a short one), so the count is bounded on both sides rather than
# reduced to whether it happened at all.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(message_format_string = 'No free connections in pool. Waiting indefinitely.') BETWEEN 1 AND 20 AS blocked_once,
        countIf(message_format_string = 'No free connections in pool. Waiting {} ms.') AS spun
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time_microseconds >= toDateTime64('${CONTEND_START}', 6)
      AND query_id LIKE '${QUERY_PREFIX}_zero_%'
      AND logger_name LIKE 'ConnectionPool%'
"

# A positive timeout keeps the finite branch, which re-enters the retry loop as soon as it expires.
# The frequency limiter is what keeps that from emitting a line per retry. Only an upper bound is
# asserted: the limiter is keyed on logger and format string with a ten second interval, so a run
# shortly after another one can legitimately see the line suppressed entirely. Without the limiter
# this reads in the thousands, so the bound still separates the two behaviours.
#
# The bound alone would also hold if a positive timeout were translated to the infinite branch,
# because that emits no line of this shape at all, so the branch is named as well.
contend 1 finite

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(message_format_string = 'No free connections in pool. Waiting {} ms.') <= 20 AS finite_wait_throttled,
        countIf(message_format_string = 'No free connections in pool. Waiting indefinitely.') AS blocked_indefinitely
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time_microseconds >= toDateTime64('${CONTEND_START}', 6)
      AND query_id LIKE '${QUERY_PREFIX}_finite_%'
      AND logger_name LIKE 'ConnectionPool%'
"
