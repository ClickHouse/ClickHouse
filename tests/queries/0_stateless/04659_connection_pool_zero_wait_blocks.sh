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

    # The killed holder is expected to fail. What the waiter is expected to do depends on the arm and
    # is asserted by the caller through $3: on the blocking arm it has to be handed the connection the
    # kill released and run to completion, because a waiter that logged the line and then blocked for
    # good, never woken, would still satisfy the log assertions below; on a finite wait it is expected
    # to give up first, so the same check has to be inverted rather than dropped.
    wait "$holder_pid" || true
    if wait "$waiter_pid"; then
        [[ $3 == succeeds ]] || echo "the waiter succeeded, so its timeout never expired"
    else
        [[ $3 == fails ]] || echo "the waiter did not get the connection back"
    fi

    [[ ${contended} == 1 ]] || echo "the queries never ran at the same time, so the pool was never full"
}

contend 0 zero succeeds

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

# A positive timeout is a deadline, so the waiter gives up rather than queueing until the holder
# releases: the `fails` argument above asserts that. The line is still logged once per wait, and only
# an upper bound is asserted on it, because the frequency limiter is keyed on logger and format string
# with a ten second interval, so a run shortly after another one can legitimately see it suppressed
# entirely. The bound alone would also hold if a positive timeout were translated to the infinite
# branch, which emits no line of this shape at all, so the branch is named as well.
# The deadline has to expire before the kill below frees the connection, or the waiter is handed it
# and succeeds, which measures the handover rather than the timeout. The kill is two seconds after
# contention is seen, and contention is seen within about a twentieth of a second, so 500 ms leaves a
# wide margin either way: the waiter gives up after three attempts of 500 ms, still well before the
# kill.
contend 500 finite fails

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

# The user-visible failure is not the raw pool error. Once an exhausted pool is a soft per-replica
# failure (ConnectionEstablisher's allowlist), it is retried per replica and the query reports
# ALL_CONNECTION_TRIES_FAILED, with the pool error as the accumulated reason. Asserting both halves is
# what separates "the timeout expired" from "the timeout expired and killed the whole query".
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        exception_code = 279 AS all_connection_tries_failed,
        position(exception, 'NO_FREE_CONNECTION') > 0 AS reason_is_the_pool_timeout,
        ProfileEvents['ConnectionPoolIsFullMicroseconds'] > 0 AS waited_on_a_full_pool
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND query_id = '${QUERY_PREFIX}_finite_waiter' AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC LIMIT 1
"

# The arms above cancel the holder, so the waiter is released by the notification the handover sends.
# Cancelling the waiter instead is what exercises the cancellation path: here the holder keeps the
# connection for the whole arm, so nothing is ever released and the wait can only end by the waiter
# noticing that its own query is over.
#
# The pool timeout is an argument because the two branches handle their deadline separately, so a
# cancellation that reaches one is no evidence about the other: the first two arms run at the default
# 0, the branch a default deployment uses, and the third at a positive timeout.
#
# Each arm asserts the holder is still running once the waiter is gone. That check is what makes the
# arm measure cancellation rather than the handover: a waiter freed by the holder releasing the
# connection would leave no holder to find. Every bound is 12 seconds against a 30 second hold, and
# reaching the pool takes about a twentieth of a second, so a loaded runner delays the start rather
# than eating the margin: the hold is a wall clock sleep, not work that can be slowed down.
function cancel_waiter() {
    local mode=$1 label=$2 pool_wait_ms=${3:-0}
    local holder="${QUERY_PREFIX}_${label}_holder" waiter="${QUERY_PREFIX}_${label}_waiter"
    local holder_pid waiter_pid contended=0 rc=0 limit=""

    # The holder does not wait, so the value is inert for it; it is kept identical to the waiter's so
    # the pair is queueing under one configuration.
    timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${holder}" --query "
        SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', numbers(30)) WHERE sleepEachRow(1)
        SETTINGS prefer_localhost_replica = 0, distributed_connections_pool_size = 1,
                 connection_pool_max_wait_ms = ${pool_wait_ms}, function_sleep_max_microseconds_per_block = 60000000
    " < /dev/null > /dev/null 2>&1 &
    holder_pid=$!

    for _ in {1..600}; do
        [[ $(running "'${holder}'") != 0 ]] && break
        sleep 0.1
    done

    # A soft deadline the pool wait has to observe by itself. A wait that does not only reports the
    # timeout once the connection comes back, which is the regression this arm pins.
    [[ ${mode} == soft ]] && limit=", max_execution_time = 5"

    timeout 12 ${CLICKHOUSE_CLIENT} --query_id "${waiter}" --query "
        SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', numbers(1))
        SETTINGS prefer_localhost_replica = 0, distributed_connections_pool_size = 1,
                 connection_pool_max_wait_ms = ${pool_wait_ms}${limit}
    " < /dev/null > /dev/null 2>&1 &
    waiter_pid=$!

    for _ in {1..600}; do
        [[ $(running "'${holder}', '${waiter}'") == 2 ]] && { contended=1; break; }
        sleep 0.05
    done

    # Being in system.processes only means the waiter has started, so give it time to reach the pool.
    sleep 2

    if [[ ${mode} == kill ]]; then
        timeout 12 ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${waiter}' SYNC" > /dev/null 2>&1 || rc=$?
        [[ ${rc} == 0 ]] || echo "the waiter did not stop when it was killed"
    fi

    wait "$waiter_pid" || rc=$?
    # 124 is the bound above firing, i.e. the waiter was still in the pool wait when it was taken out.
    [[ ${rc} != 124 ]] || echo "the waiter stayed in the pool wait past its own end"

    [[ $(running "'${holder}'") != 0 ]] || echo "the holder finished first, so the waiter was freed by the handover"
    [[ ${contended} == 1 ]] || echo "the queries never ran at the same time, so the pool was never full"

    # shellcheck disable=SC2086
    timeout 30 ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${holder}' ASYNC" > /dev/null
    wait "$holder_pid" || true
}

cancel_waiter kill cancelled
cancel_waiter soft softlimit

# A positive timeout keeps its deadline in a branch of its own, so the two arms above say nothing
# about it: a wait that consulted the deadline once and then slept straight to it would pass both.
# Five minutes is far past this arm's own twelve second bound, so only the kill can end the wait
# within it, and an unsliced wait to that deadline shows up as the bound firing.
cancel_waiter kill cancelledfinite 300000

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# The codes are pinned as well as the timing, so an arm that stopped reporting the cancellation and
# started reporting something else (a pool error of its own, say) is not silently accepted. One column
# per arm, so a single arm regressing stays legible.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(query_id = '${QUERY_PREFIX}_cancelled_waiter' AND exception_code = 394) AS cancelled_by_kill,
        countIf(query_id = '${QUERY_PREFIX}_softlimit_waiter' AND exception_code = 159) AS stopped_by_time_limit,
        countIf(query_id = '${QUERY_PREFIX}_cancelledfinite_waiter' AND exception_code = 394) AS finite_cancelled_by_kill
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND query_id IN ('${QUERY_PREFIX}_cancelled_waiter', '${QUERY_PREFIX}_softlimit_waiter',
                       '${QUERY_PREFIX}_cancelledfinite_waiter')
      AND type != 'QueryStart'
"
