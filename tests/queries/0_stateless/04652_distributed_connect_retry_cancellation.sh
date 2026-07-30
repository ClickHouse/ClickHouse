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
#
# Measured in milliseconds, not with $SECONDS. $SECONDS counts tick boundaries crossed rather than
# time elapsed, so a sub-second refusal that straddles a tick reads as 1 and would be accepted as
# blocking -- silently, in the one direction that matters. A refusal takes ~0.1 s here and a genuine
# block ~2.1 s against the 2000 ms timeout, so 1000 ms sits an order of magnitude above the former
# and half of the latter.
probe_start_ns=$(date +%s%N)
$CLICKHOUSE_CLIENT --connections_with_failover_max_tries 1 \
                   --connect_timeout_with_failover_ms 2000 \
                   --query "SELECT count() FROM remote('198.51.100.1', system.one) FORMAT Null" >/dev/null 2>&1
probe_ms=$(( ($(date +%s%N) - probe_start_ns) / 1000000 ))
if [ "$probe_ms" -lt 1000 ]; then
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
    # attempt": it admits 6 extra dials. Count the attempts instead. The ceiling is what the fixture
    # permits, not a round number: each connect takes the full 2 s timeout, so against the 3 s
    # deadline only the attempts starting at t~0 and t~2 can begin before cancellation -- at most 2.
    # An upper bound rather than an equality, because the deadline can land either between attempts
    # or during one. The pre-fix loop performs 120 attempts, so 2 separates the two behaviours by 60x.
    local attempts
    attempts=$(attempts_of "$qid")
    [ -n "$attempts" ] && [ "$attempts" -le 2 ] && echo "$label stopped within 2 attempts" \
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
# The 1 s deadline against a 5 s connect is what makes "during the attempt" reliable rather than
# lucky: cancellation is aligned to a 100 ms grid and rounded up, never down
# (CancellationChecker.cpp:15,102), so the ~4 s of slack absorbs the grid many times over.
#
# Synchronous path only. On the asynchronous (hedged) path the terminal attempt finishes outside the
# fiber, in ConnectionEstablisherAsync::checkTimeout, whose no-more-addresses branch returns false;
# AsyncTaskExecutor::resume then returns early (AsyncTaskExecutor.cpp:22-23) without resuming the
# fiber, so the establisher's catch is structurally unreachable there -- not merely prevented from
# throwing. That is why this case pins use_hedged_requests to 0.
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
                   --log_queries 1 \
                   --log_profile_events 1 \
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
    # How many attempts had already happened when the kill was sent. A fixed ceiling cannot be
    # derived here the way it can for the deadline cases: the kill goes out only once the query is
    # visible in system.processes, so the poll latency decides how many 2 s connects are already
    # behind us. Measuring the count first turns the assertion below into a delta, which is what the
    # fix is actually about -- how many more replicas get dialed AFTER cancellation.
    kill_attempts_before=$($CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['DistributedConnectionTries']
        FROM system.processes WHERE query_id = '$kill_qid' LIMIT 1")

    kill_start=$SECONDS
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$kill_qid' SYNC" >/dev/null 2>&1
    wait "$kill_client_pid" 2>/dev/null
    kill_elapsed=$((SECONDS - kill_start))

    [ "$kill_elapsed" -lt 15 ] && echo "kill stopped early" || echo "kill still dialing after ${kill_elapsed}s"
    # QUERY_WAS_CANCELLED, not TIMEOUT_EXCEEDED: this case sets no deadline, so a TIMEOUT_EXCEEDED
    # here would mean the assertion is measuring the wrong mechanism.
    grep -qF 'QUERY_WAS_CANCELLED' "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err" && echo "kill reported as cancelled" \
        || echo "kill wrong error: $(cat "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err")"

    # The 15 s bound above still tolerates ~6 further dials, so give the kill path the same attempt
    # oracle the deadline cases have. At most one more attempt may complete: the one already in
    # flight when the kill lands. Anything beyond that is the loop dialing on past a cancelled query,
    # which is 120 attempts without the fix.
    kill_attempts_after=$(attempts_of "$kill_qid")
    if [ -n "$kill_attempts_before" ] && [ -n "$kill_attempts_after" ] \
       && [ "$((kill_attempts_after - kill_attempts_before))" -le 1 ]; then
        echo "kill stopped within 1 further attempt"
    else
        echo "kill went from $kill_attempts_before to $kill_attempts_after attempts"
    fi
fi
rm -f "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err"
