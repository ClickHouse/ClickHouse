#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 198.51.100.0/24 is RFC 5737 TEST-NET-2, reserved for documentation. RFC 5737 section 4 recommends
# filtering such traffic but does not mandate silently dropping it, so the assumption this test rests
# on -- that a connect blocks for the full connect timeout instead of being refused -- is verified
# below before anything relies on it. 5 addresses x 3 tries x 2 s is 30 s of dialing, against a 3 s
# max_execution_time: without a cancellation checkpoint in the connect-retry loop the query grinds
# through all of it and reports ALL_CONNECTION_TRIES_FAILED instead of the timeout.
#
# use_hedged_requests selects a different IConnections implementation and is randomized by the test
# runner, so both values are pinned and run as separate cases. Settings go on the client command
# line, which survives a randomized `compatibility` setting.

# Both helpers read one field off the query_log row for one query. These queries fail during
# analysis, so the row is an ExceptionBeforeStart one; its duration and performance counters are
# still finalized and attached to it.
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

duration_of()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT query_duration_ms
        FROM system.query_log
        WHERE query_id = '$1' AND type = 'ExceptionBeforeStart' AND current_database = currentDatabase()
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

# Precondition: connecting to the reserved range must BLOCK. On a host that refuses instead, all
# tries finish inside the deadline, the query reports the exhausted-retries error before any
# cancellation exists, and every case below would fail with a misleading "wrong error". Detect that
# here and say so; the missing reference lines then make the test fail loudly and legibly.
#
# WHAT IS MEASURED, because the ceiling below needs the per-attempt duration D and not something
# larger: the SERVER-side `query_duration_ms` of the probe query, read back from its query_log row.
# That figure comes from a stopwatch started inside executeQueryImpl (executeQuery.cpp:1195,
# reported at :2099, stored at :929), so client startup, connection to the local server, exception
# propagation and output handling are all OUTSIDE it. Timing the `clickhouse-client` invocation
# instead would fold in ~80-105 ms of such overhead (measured here), and overhead can only make the
# figure LARGER, which is the one direction that matters: it would accept a D that far below the
# threshold. With one address and one try, this figure is one connect plus analysis.
#
# The threshold is then derived, not picked:
#   * Attempts are sequential, starting at t = 0, D, 2D, ..., and cancellation for run_case's 3 s
#     deadline lands no later than 3000 + CANCELLATION_GRID_MS - 1 = 3099 ms, because the deadline is
#     rounded UP to the next 100 ms grid boundary and never down (CancellationChecker.cpp:15,102).
#     At most 2 attempts start iff 2 * D >= 3099, i.e. D >= 1549.5 ms; below that a correct
#     implementation may legitimately begin a third one and redden the ceiling.
#   * The figure still exceeds D by the analysis remainder eps, so accepting on `figure >= threshold`
#     accepts D >= threshold - eps, and the threshold must therefore be at least 1549.5 + eps. eps
#     was measured against a refusing endpoint, where D is ~0 and the figure is eps alone: at most
#     1 ms over 40 trials (20 idle, 20 at load average 110), consistent with 2001 ms against the
#     2000 ms configured timeout on 12 of 12 blocking trials. Hence 1549.5 + 1, rounded up to 1551.
# The fixture's two behaviours are separated by three orders of magnitude either side: a refusal
# measures 1 ms (1551x below the threshold), a genuine block 2000-2036 ms (1.29x above).
probe_qid="${CLICKHOUSE_DATABASE}_probe_$RANDOM"
$CLICKHOUSE_CLIENT --connections_with_failover_max_tries 1 \
                   --connect_timeout_with_failover_ms 2000 \
                   --log_queries 1 \
                   --log_profile_events 1 \
                   --query_id "$probe_qid" \
                   --query "SELECT count() FROM remote('198.51.100.1', system.one) FORMAT Null" >/dev/null 2>&1
probe_ms=$(duration_of "$probe_qid")
if [ -z "$probe_ms" ]; then
    # Not a fixture verdict: the probe's own duration could not be read, so the precondition is
    # simply unknown and every case below would be unsafe to trust either way.
    echo "fixture undecidable: no query_log duration for the precondition probe"
    exit 0
fi
if [ "$probe_ms" -lt 1551 ]; then
    echo "fixture unusable: 198.51.100.1 does not block, it is refused"
    exit 0
fi

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
                               --query "SELECT count() FROM remote('198.51.100.{1..5}', system.one) FORMAT Null" 2>&1)
    local elapsed=$((SECONDS - start))

    # A bound, not a value: the post-fix window is the 3 s deadline plus at most one 2 s connect
    # already in flight, and every extra dial adds another 2 s. 15 s therefore tolerates at most
    # (15-3)/2 = 6 extra dials while keeping ~3x margin over the observed elapsed, against 30 s of
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
    # or during one. The pre-fix loop performs 15 attempts, so 2 separates the two behaviours by 7.5x.
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

# The precondition probe above measured a 2 s connect, which bounds the deadline cases but not this
# one: a fixture that reports an error after, say, 2.5 s clears that probe yet ends this case's
# nominal 8 s attempt early, letting a correct query begin attempt 2 before the kill is issued. Since
# the margin claimed below is a property of the 8 s configuration specifically, measure that
# configuration. Require most of it (6 s of the 8 s) so the check tests the fixture rather than host
# jitter; a genuine block measures 8001-8040 ms here.
kill_probe_qid="${CLICKHOUSE_DATABASE}_kill_probe_$RANDOM"
$CLICKHOUSE_CLIENT --connections_with_failover_max_tries 1 \
                   --connect_timeout_with_failover_ms 8000 \
                   --log_queries 1 \
                   --log_profile_events 1 \
                   --query_id "$kill_probe_qid" \
                   --query "SELECT count() FROM remote('198.51.100.1', system.one) FORMAT Null" >/dev/null 2>&1
kill_probe_ms=$(duration_of "$kill_probe_qid")
if [ -z "$kill_probe_ms" ] || [ "$kill_probe_ms" -lt 6000 ]; then
    echo "kill case unusable: an 8s connect to 198.51.100.1 lasted ${kill_probe_ms:-unknown}ms"
    exit 0
fi

# max_execution_time and KILL QUERY reach the establisher through different reasons and therefore
# different error codes: the deadline yields TIMEOUT_EXCEEDED, an explicit kill yields
# QUERY_WAS_CANCELLED. An implementation that only observed the deadline would pass every case above,
# so drive the kill path explicitly. No max_execution_time here, so the kill is the only thing that
# can stop the query. Background query plus poll plus KILL ... WHERE query_id follows
# 01950_kill_large_group_by_query.sh. use_hedged_requests is pinned to the synchronous path to keep
# the runtime bounded; the hedged branch is already covered by the deadline case above.
#
# The connect timeout is 8 s here rather than the 2 s used above, and the attempt count is read from
# query_log AFTER the query is gone rather than sampled live from system.processes. Both follow from
# what stays observable. The oracle needs the number of attempts the query performed in total, and
# the state that carries it in system.processes is only readable while the query is still running --
# a window the fix deliberately shrinks to the single connect that was already in flight when the
# kill landed (measured 1836-2063 ms with a 2 s timeout). Polling for that window is a race, and it
# is a race the fix makes tighter, so it fails most often precisely where the fix works best. The
# query_log row is written once the query ends and is then readable indefinitely, so the assertion
# below no longer has a deadline. One connect of 8 s then leaves ~8 s for the kill to land inside
# attempt 1, against a kill that completes in 147-333 ms -- a margin of more than 20x, which is what
# makes "the kill landed during the first attempt" reliable rather than lucky.
kill_qid="${CLICKHOUSE_DATABASE}_kill_$RANDOM"
$CLICKHOUSE_CLIENT --use_hedged_requests 0 \
                   --connections_with_failover_max_tries 3 \
                   --connect_timeout_with_failover_ms 8000 \
                   --log_queries 1 \
                   --log_profile_events 1 \
                   --query_id "$kill_qid" \
                   --query "SELECT count() FROM remote('198.51.100.{1..5}', system.one) FORMAT Null" \
                   >/dev/null 2>"${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err" &
kill_client_pid=$!

# Wait for the first attempt to have STARTED, not merely for the query to exist. A query is
# registered in system.processes by executeQueryImpl (executeQuery.cpp:1574) before analysis builds
# the remote table, so there is an interval in which it is visible with no connection attempt yet
# begun. A kill delivered inside that interval is handled correctly -- the loop-top checkpoint stops
# the query before the first connect -- but it leaves a total of zero attempts, which is not the
# number asserted below. Waiting for the counter to reach 1 removes that ambiguity, so the assertion
# can be an exact equality rather than a bound. Locally the counter already reads 1 at the first
# sighting in 14 of 14 trials; on a loaded sanitizer runner the interval is wider, which is exactly
# where this test has been unstable.
#
# Unlike the cancellation flag, both the query's presence and its attempt counter stay observable for
# as long as it dials: pre-fix ~120 s, post-fix at least the 8 s of attempt 1.
attempt_started=0
for _ in {1..100}; do
    if [ "$($CLICKHOUSE_CLIENT --query "
                SELECT count() FROM system.processes
                WHERE query_id = '$kill_qid' AND ProfileEvents['DistributedConnectionTries'] >= 1")" = "1" ]; then
        attempt_started=1
        break
    fi
    sleep 0.1
done

if [ "$attempt_started" = "0" ]; then
    echo "kill case unusable: query never began a connection attempt"
    # Stop the query SERVER-side, then reap the client. Signalling the client is not enough on its
    # own: it installs no SIGTERM handler (ClientBase.cpp:728-730 covers only SIGPIPE and SIGQUIT), so
    # it dies without sending a Cancel packet, and during analysis the server is synchronously inside
    # executeQuery and does not notice the closed socket. Verified: 17 s after signalling and reaping
    # the client the query was still in system.processes with is_cancelled = 0. Leaving it there would
    # let it dial for the ~120 s below, into later tests and into the hung check.
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$kill_qid' ASYNC" >/dev/null 2>&1
    kill "$kill_client_pid" 2>/dev/null
    wait "$kill_client_pid" 2>/dev/null
else
    kill_start=$SECONDS
    # ASYNC, not SYNC: its branch calls sendCancelToQuery inline before returning
    # (InterpreterKillQueryQuery.cpp:231-243), so the cancellation is registered by the time this
    # statement completes, and it returns without waiting for the query to disappear.
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$kill_qid' ASYNC" >/dev/null 2>&1

    wait "$kill_client_pid" 2>/dev/null
    kill_elapsed=$((SECONDS - kill_start))

    # One attempt of 8 s plus the kill, against the ~120 s this case dials without the fix.
    [ "$kill_elapsed" -lt 30 ] && echo "kill stopped early" || echo "kill still dialing after ${kill_elapsed}s"
    # QUERY_WAS_CANCELLED, not TIMEOUT_EXCEEDED: this case sets no deadline, so a TIMEOUT_EXCEEDED
    # here would mean the assertion is measuring the wrong mechanism.
    grep -qF 'QUERY_WAS_CANCELLED' "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err" && echo "kill reported as cancelled" \
        || echo "kill wrong error: $(cat "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err")"

    # The elapsed bound above still tolerates further dials, so count the attempts as the deadline
    # cases do. DistributedConnectionTries is incremented as the FIRST statement of try_establish
    # (ConnectionEstablisher.cpp:72), before the connect, so it counts attempts that START, and the
    # kill lands within ~0.3 s of attempt 1 beginning an 8 s connect. A total of 1 therefore means no
    # attempt was ever begun on a query already known to be cancelled, which is what the fix exists
    # to guarantee. Without the fix the query dials every remaining address, 15 attempts in all.
    kill_attempts=$(attempts_of "$kill_qid")
    if [ -n "$kill_attempts" ] && [ "$kill_attempts" -eq 1 ]; then
        echo "kill started no further attempts"
    else
        echo "kill used $kill_attempts attempts"
    fi
fi
rm -f "${CLICKHOUSE_TMP}/04652_kill_${kill_qid}.err"
