#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 198.51.100.0/24 is RFC 5737 TEST-NET-2, reserved for documentation. RFC 5737 section 4 recommends
# filtering such traffic but does not mandate silently dropping it, so the assumption this test rests
# on -- that a connect blocks for the full connect timeout instead of being refused -- is verified
# below before anything relies on it. 40 addresses x 3 tries x 2 s is 240 s of dialing, against a 3 s
# max_execution_time: without a cancellation checkpoint in the connect-retry loop the query grinds
# through all of it and reports ALL_CONNECTION_TRIES_FAILED instead of the timeout.
#
# use_hedged_requests selects a different IConnections implementation and is randomized by the test
# runner, so both values are pinned and run as separate cases. Settings go on the client command
# line, which survives a randomized `compatibility` setting.

# Precondition: connecting to the reserved range must BLOCK. On a host that refuses instead, all
# tries finish inside the deadline, the query reports the exhausted-retries error before any
# cancellation exists, and every case below would fail with a misleading "wrong error". Detect that
# here and say so; the missing reference lines then make the test fail loudly and legibly.
probe_start=$SECONDS
$CLICKHOUSE_CLIENT --connections_with_failover_max_tries 1 \
                   --connect_timeout_with_failover_ms 2000 \
                   --query "SELECT count() FROM remote('198.51.100.1', system.one) FORMAT Null" >/dev/null 2>&1
if [ $((SECONDS - probe_start)) -lt 1 ]; then
    echo "fixture unusable: 198.51.100.1 does not block, it is refused"
    exit 0
fi

# Reads ProfileEvents off the query_log row for one query. The query fails during analysis, so the
# row is an ExceptionBeforeStart one; performance counters are still finalized and attached to it.
attempts_of()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['DistributedConnectionTries']
        FROM system.query_log
        WHERE query_id = '$1' AND type = 'ExceptionBeforeStart' AND current_database = currentDatabase()
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

run_case()
{
    local label=$1
    local hedged=$2
    local qid="${CLICKHOUSE_DATABASE}_${label}_$RANDOM"
    local start=$SECONDS
    local error
    error=$($CLICKHOUSE_CLIENT --use_hedged_requests "$hedged" \
                               --connections_with_failover_max_tries 3 \
                               --connect_timeout_with_failover_ms 2000 \
                               --max_execution_time 3 \
                               --log_queries 1 \
                               --log_profile_events 1 \
                               --query_id "$qid" \
                               --query "SELECT count() FROM remote('198.51.100.{1..40}', system.one) FORMAT Null" 2>&1)
    local elapsed=$((SECONDS - start))

    # A bound, not a value: the post-fix window is the 3 s deadline plus at most one 2 s connect
    # already in flight, and every extra dial adds another 2 s. 15 s therefore tolerates at most
    # (15-3)/2 = 6 extra dials while keeping ~3x margin over the observed elapsed, against 240 s of
    # pre-fix dialing.
    [ "$elapsed" -lt 15 ] && echo "$label stopped early" || echo "$label still dialing after ${elapsed}s"

    # The reported code must be the cancellation, not the exhausted-retries outcome. TIMEOUT_EXCEEDED
    # is a plain Exception, so it is not swallowed by the caller's `catch (const NetException &)`
    # next-shard retry in getStructureOfRemoteTable.
    echo "$error" | grep -qF 'TIMEOUT_EXCEEDED' && echo "$label reported as timeout" || echo "$label wrong error: $error"

    # The elapsed bound alone cannot distinguish "stops at the next attempt" from "stops every Nth
    # attempt": it admits 6 extra dials. Count the attempts instead. An upper bound rather than an
    # exact value, because the deadline can land either between attempts or during one. The pre-fix
    # loop performs 120 attempts, so 4 separates the two behaviours by 30x.
    local attempts
    attempts=$(attempts_of "$qid")
    [ -n "$attempts" ] && [ "$attempts" -le 4 ] && echo "$label stopped within 4 attempts" \
        || echo "$label used $attempts attempts"
}

run_case sync 0
run_case hedged 1

# The checks above are all reached through the top of the attempt loop, which the final attempt
# cannot reach again. This case closes that window: one address, one try, and a deadline shorter than
# the connect timeout, so cancellation necessarily lands DURING the one and only attempt. Without a
# checkpoint on the establisher's terminal-failure path the caller reports
# ALL_CONNECTION_TRIES_FAILED, which getStructureOfRemoteTable then converts into a next-shard retry.
#
# Synchronous path only: the asynchronous (hedged) establisher finishes such an attempt outside the
# fiber, in ConnectionEstablisherAsync::checkTimeout, which is documented as unable to throw, so it
# does not pass through the establisher's catch and is not covered here.
terminal_qid="${CLICKHOUSE_DATABASE}_terminal_$RANDOM"
terminal_error=$($CLICKHOUSE_CLIENT --use_hedged_requests 0 \
                                    --connections_with_failover_max_tries 1 \
                                    --connect_timeout_with_failover_ms 5000 \
                                    --max_execution_time 1 \
                                    --query_id "$terminal_qid" \
                                    --query "SELECT count() FROM remote('198.51.100.1', system.one) FORMAT Null" 2>&1)
echo "$terminal_error" | grep -qF 'TIMEOUT_EXCEEDED' && echo "terminal reported as timeout" \
    || echo "terminal wrong error: $terminal_error"

# max_execution_time and KILL QUERY reach the establisher through different reasons and therefore
# different error codes: the deadline yields TIMEOUT_EXCEEDED, an explicit kill yields
# QUERY_WAS_CANCELLED. An implementation that only observed the deadline would pass every case above,
# so drive the kill path explicitly. No max_execution_time here, so the kill is the only thing that
# can stop the query. Background query plus poll plus KILL ... WHERE query_id follows
# 01950_kill_large_group_by_query.sh. use_hedged_requests is pinned to the synchronous path to keep
# the runtime bounded; the hedged branch is already covered by the deadline case above.
kill_qid="${CLICKHOUSE_DATABASE}_kill_$RANDOM"
$CLICKHOUSE_CLIENT --use_hedged_requests 0 \
                   --connections_with_failover_max_tries 3 \
                   --connect_timeout_with_failover_ms 2000 \
                   --query_id "$kill_qid" \
                   --query "SELECT count() FROM remote('198.51.100.{1..40}', system.one) FORMAT Null" \
                   >/dev/null 2>"${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err" &
kill_client_pid=$!

appeared=0
for _ in {1..100}; do
    if [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$kill_qid'")" != "0" ]; then
        appeared=1
        break
    fi
    sleep 0.1
done

if [ "$appeared" = "0" ]; then
    echo "kill case unusable: query never appeared in system.processes"
else
    kill_start=$SECONDS
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$kill_qid' SYNC" >/dev/null 2>&1
    wait "$kill_client_pid" 2>/dev/null
    kill_elapsed=$((SECONDS - kill_start))

    [ "$kill_elapsed" -lt 15 ] && echo "kill stopped early" || echo "kill still dialing after ${kill_elapsed}s"
    # QUERY_WAS_CANCELLED, not TIMEOUT_EXCEEDED: this case sets no deadline, so a TIMEOUT_EXCEEDED
    # here would mean the assertion is measuring the wrong mechanism.
    grep -qF 'QUERY_WAS_CANCELLED' "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err" && echo "kill reported as cancelled" \
        || echo "kill wrong error: $(cat "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err")"
fi
rm -f "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err"
