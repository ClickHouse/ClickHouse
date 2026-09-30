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

# Bounded by wall clock rather than by a number of attempts: one poll spawns a client, which is the
# dominant cost here and takes seconds under a sanitizer, so an attempt count bounds no amount of
# time. A condition that can no longer hold has to be reported here, well inside the test budget.
function wait_running() {
    local wanted=$1 ids=$2 deadline=$((SECONDS + $3))
    while (( SECONDS < deadline )); do
        [[ $(running "${ids}") == "${wanted}" ]] && return 0
        sleep 0.05
    done
    return 1
}

# Runs one pair of queries at the given connection_pool_max_wait_ms and leaves the log rows the
# assertion below reads. The holder does not finish on its own: it is killed only once the arm's own
# expected event has been observed, so the pool is full for the whole of the waiter's wait and no
# attempt has to be repeated to land that overlap.
function contend() {
    local wait_ms=$1 label=$2
    local holder="${QUERY_PREFIX}_${label}_holder" waiter="${QUERY_PREFIX}_${label}_waiter"
    local holder_pid waiter_pid

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

    # Only the holder can free the connection, so the waiter starts once the holder owns it.
    wait_running 1 "'${holder}'" 60 || echo "the holder never started, so the pool was never full"

    timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${waiter}" --query "
        SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', numbers(1)) WHERE sleepEachRow(1)
        SETTINGS prefer_localhost_replica = 0, distributed_connections_pool_size = 1,
                 connection_pool_max_wait_ms = ${wait_ms}, function_sleep_max_microseconds_per_block = 60000000
    " < /dev/null > /dev/null 2>&1 &
    waiter_pid=$!

    # What ends the waiter's wait differs by arm, so what has to be observed before the kill does
    # too. The two are ordered rather than timed: whichever event the arm expects is waited for, so
    # neither depends on the kill landing inside a window.
    local waiter_rc=0
    if [[ $3 == succeeds ]]; then
        # Here only the kill can end the wait, so the waiter has to be in it first: a kill that
        # landed earlier would free the connection before there was any wait to hand it over to.
        wait_running 1 "'${waiter}'" 60 || echo "the waiter never started, so it never reached the pool"

        # Being in system.processes only means the waiter has started, so give it time to reach the
        # pool and log.
        sleep 2
    else
        # Here the waiter ends on its own deadline, which is the whole point of the arm, so the kill
        # must not come first: waiting for the waiter to exit is what orders the two. Its own bound
        # caps this, and the retries it logs on the way out are what the assertion below reads.
        wait "$waiter_pid" || waiter_rc=$?
    fi

    # The holder is the only connection holder, so it still running here is what made the pool full
    # for the whole of the waiter's wait. On the arm above the waiter has already given up, so this
    # also separates a waiter that hit its deadline from one the holder released early.
    [[ $(running "'${holder}'") != 0 ]] || echo "the holder released before the waiter, so the pool was never full"

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
    [[ $3 == fails ]] || wait "$waiter_pid" || waiter_rc=$?
    if [[ ${waiter_rc} == 0 ]]; then
        [[ $3 == succeeds ]] || echo "the waiter succeeded, so its timeout never expired"
    else
        [[ $3 == fails ]] || echo "the waiter did not get the connection back"
    fi
    # 124 is the waiter's own bound firing, i.e. it was still in the pool wait when it was taken out,
    # which neither arm expects: the blocking one is handed the connection by the kill and the finite
    # one gives up on its deadline.
    [[ ${waiter_rc} != 124 ]] || echo "the waiter was still in the pool wait when its bound fired"
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
# The deadline has to expire before the kill frees the connection, or the waiter is handed it and
# succeeds, which measures the handover rather than the timeout. That ordering is waited for rather
# than timed, so the value only has to be short against the holder's thirty second hold.
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
    local holder_pid waiter_pid rc=0 limit=""

    # The holder does not wait, so the value is inert for it; it is kept identical to the waiter's so
    # the pair is queueing under one configuration.
    timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${holder}" --query "
        SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', numbers(30)) WHERE sleepEachRow(1)
        SETTINGS prefer_localhost_replica = 0, distributed_connections_pool_size = 1,
                 connection_pool_max_wait_ms = ${pool_wait_ms}, function_sleep_max_microseconds_per_block = 60000000
    " < /dev/null > /dev/null 2>&1 &
    holder_pid=$!

    wait_running 1 "'${holder}'" 60 || echo "the holder never started, so the pool was never full"

    # A soft deadline the pool wait has to observe by itself. A wait that does not only reports the
    # timeout once the connection comes back, which is the regression this arm pins.
    [[ ${mode} == soft ]] && limit=", max_execution_time = 5"

    timeout 12 ${CLICKHOUSE_CLIENT} --query_id "${waiter}" --query "
        SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', numbers(1))
        SETTINGS prefer_localhost_replica = 0, distributed_connections_pool_size = 1,
                 connection_pool_max_wait_ms = ${pool_wait_ms}${limit}
    " < /dev/null > /dev/null 2>&1 &
    waiter_pid=$!

    if [[ ${mode} == kill ]]; then
        # Sound here: nothing releases the connection, so this waiter stays in the wait until the
        # kill below ends it, and the overlap is observable at any polling granularity.
        wait_running 2 "'${holder}', '${waiter}'" 60 \
            || echo "the queries never ran at the same time, so the pool was never full"

        # Being in system.processes only means the waiter has started, so give it time to reach the
        # pool: a kill that arrives before the wait would not exercise the wait at all.
        sleep 2

        timeout 12 ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${waiter}' SYNC" > /dev/null 2>&1 || rc=$?
        [[ ${rc} == 0 ]] || echo "the waiter did not stop when it was killed"
    fi

    # This waiter ends on its own limit rather than on anything done to it, so its whole lifetime is
    # bounded and polling for it while it lasts would sample a window that can be shorter than one
    # poll. It is waited for instead, and that it reached the pool at all is asserted below from the
    # wait it accumulated, which outlives the wait itself.
    wait "$waiter_pid" || rc=$?
    # 124 is the bound above firing, i.e. the waiter was still in the pool wait when it was taken out.
    [[ ${rc} != 124 ]] || echo "the waiter stayed in the pool wait past its own end"

    [[ $(running "'${holder}'") != 0 ]] || echo "the holder finished first, so the waiter was freed by the handover"

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
#
# Each arm also has to have reached the pool, or its code would say only that the query was stopped,
# not that a pool wait was what got stopped. The accumulated wait is what says so, and unlike being
# in system.processes it is still readable after the waiter has gone, so it holds for the arm that
# ends on its own limit as much as for the ones that are killed.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(query_id = '${QUERY_PREFIX}_cancelled_waiter' AND exception_code = 394) AS cancelled_by_kill,
        countIf(query_id = '${QUERY_PREFIX}_softlimit_waiter' AND exception_code = 159) AS stopped_by_time_limit,
        countIf(query_id = '${QUERY_PREFIX}_cancelledfinite_waiter' AND exception_code = 394) AS finite_cancelled_by_kill,
        countIf(ProfileEvents['ConnectionPoolIsFullMicroseconds'] > 0) AS all_three_waited_on_a_full_pool
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND query_id IN ('${QUERY_PREFIX}_cancelled_waiter', '${QUERY_PREFIX}_softlimit_waiter',
                       '${QUERY_PREFIX}_cancelledfinite_waiter')
      AND type != 'QueryStart'
"
