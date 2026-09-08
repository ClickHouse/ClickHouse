#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, shard
# Tag no-parallel: the failpoints are server-global and this test *waits* on them, so a concurrent
#   instance could satisfy this instance's `SYSTEM WAIT FAILPOINT ... PAUSE` from its own executor.
#   `PAUSEABLE_ONCE` bounds parks per arming, not across concurrent instances.
# Tag no-fasttest: Fast test allows 60 s per test and abandons the run at 126 s; this test synchronises
#   four failpoints, so a rig where the fixture cannot form spends its bounded diagnostics instead. The
#   regular stateless jobs allow 600 s, which is where it belongs.
# Tag shard: uses a two-shards Distributed table.

# Regression test for `RemoteQueryExecutor::cancel` waiting for `finish`'s packet drain. `finish`
# holds `was_cancelled_mutex` across an unbounded blocking `receivePacket`, and
# `ExecutingGraph::cancel` calls `cancel` on every processor in turn under `processors_mutex`, so one
# draining remote source used to stall cancellation of the whole pipeline.
#
# Both scenarios build the same fixture: fp `receive_packet_pause` parks one shard's reader before it
# consumes a packet, so that shard's query is still pending when `LIMIT 1` closes the output ports and
# `onUpdatePorts` drives `finish` on its executor. That shard is called the parked shard below; which
# of the two it is, is decided by whichever reader reaches the one-shot failpoint first, and every
# other failpoint here is guarded by the same executor-local `in_receive_packet_window` predicate, so
# the sibling shard can never consume a park this test is waiting for.
# `async_socket_for_remote=0` picks the synchronous read path, which is where `receive_packet_pause`
# lives; the drain is synchronous either way.
#
# Scenario A: `finish` parks at the start of its drain, holding `was_cancelled_mutex`, and
# `KILL QUERY` has to return anyway. That is an ordering assertion, not a duration one: while the park
# is held the unfixed latency is unbounded, so the bound below only has to exceed a normal
# `KILL QUERY` round-trip.
#
# Scenario B: the reverse order, which is the interleaving the first version of this fix left open.
# `cancel` arrives *before* `finish` has announced itself, so a check-then-lock `cancel` would fall
# through and then wait out the whole drain. It asserts both directions of that: `finish` must not
# reach its drain while `cancel` holds the gate, and the `KILL` must complete once the gate is free
# even though the drain park is still armed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP_RECV="remote_query_executor_receive_packet_pause"
FP_HOLD="remote_query_executor_finish_drain_hold"
FP_ENTRY="remote_query_executor_finish_entry_hold"
FP_GATE="remote_query_executor_cancel_gate_hold"

# Every bare `wait` below must be reachable only with all parks released: a client left parked would
# block it until the runner's timeout, with no diagnosis.
function release_all()
{
    for fp in "$FP_HOLD" "$FP_ENTRY" "$FP_GATE" "$FP_RECV"; do
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $fp" 2>/dev/null ||:
    done
}

function cleanup()
{
    release_all
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${CLICKHOUSE_DATABASE}.src SELECT number FROM numbers(100000);
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dist AS ${CLICKHOUSE_DATABASE}.src
        ENGINE = Distributed(test_cluster_two_shards, ${CLICKHOUSE_DATABASE}, src);
"

failed=0
sync_ok=0
err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
# The background query clients get their own stderr path: they run concurrently with the foreground
# helpers below, and both truncate on open, so sharing one file loses whichever diagnostic came first.
err_bg="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.bg.err"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_drain_hold"

# Fail loudly if a failpoint is not available: a run that proceeds un-armed proves nothing.
function arm()
{
    if ! $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $1" 2>"$err"; then
        echo "cannot arm failpoint $1:"
        cat "$err"
        failed=1
        return 1
    fi
}

# `SYSTEM WAIT FAILPOINT ... PAUSE` blocks until someone parks, so every wait here is bounded: a
# configuration in which the fixture cannot form must fail with a diagnosis, not sit until the
# runner's own timeout kills the test.
function wait_pause()
{
    local status
    timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $1 PAUSE" 2>"$err"
    status=$?
    if [ "$status" -eq 0 ]; then
        return 0
    fi
    if [ "$status" -eq 124 ]; then
        echo "failpoint $1 was never reached, so the interleaving was never established"
    else
        echo "wait for failpoint $1 failed:"
        cat "$err"
    fi
    return 1
}

# `LIMIT 1` without `ORDER BY`: the shard that is not parked delivers a row and closes the output
# ports, so `onUpdatePorts` calls `finish` on the parked shard's executor and reaches the drain.
# `enable_parallel_replicas=0` keeps `drain_was_skipped` false, which is what leads into the drain.
# `--max_threads` is pinned: the interleaving needs both shards' readers runnable at once, so do not
# let a randomized thread count decide whether the fixture is reachable.
# Bounded so the bare `wait`s below cannot outlive the runner: 120 s is ~10x the whole test.
function start_query()
{
    timeout 120 $CLICKHOUSE_CLIENT \
        --query_id "$1" \
        --enable_parallel_replicas=0 --async_socket_for_remote=0 \
        --max_block_size=1 --prefer_localhost_replica=0 --max_threads=2 \
        --query "SELECT x FROM ${CLICKHOUSE_DATABASE}.dist LIMIT 1 FORMAT Null" 2>"$err_bg" &
}

# The killed query must actually be gone once the parks are released.
function assert_query_gone()
{
    for _ in {1..100}; do
        if [ "$($CLICKHOUSE_CLIENT --query "
                SELECT count() FROM system.processes WHERE query_id = '$1'")" = "0" ]; then
            return 0
        fi
        sleep 0.3
    done
    echo "query $1 is still running after the failpoints were released"
    failed=1
}

########## Scenario A: cancel arrives while finish is already parked in its drain ##########

armed=0
arm "$FP_RECV" && arm "$FP_HOLD" && armed=1

if [ "$armed" -eq 1 ]; then
    start_query "$query_id"

    # Without both parks the interleaving never happened and the assertion below would be vacuous.
    sync_ok=1
    for fp in "$FP_RECV" "$FP_HOLD"; do
        if ! wait_pause "$fp"; then
            failed=1
            sync_ok=0
            # The fixture is already broken and `wait_pause` has already named the failpoint; a second
            # 30 s wait cannot add information, and the runner's budget is finite.
            break
        fi
    done
fi

if [ "$sync_ok" -eq 1 ]; then
    # A thread is now parked inside `finish` holding `was_cancelled_mutex`. Asynchronous
    # `KILL QUERY` calls `ProcessList::sendCancelToQuery` in its own thread, which drives
    # `ExecutingGraph::cancel` down to `RemoteQueryExecutor::cancel` on that executor.
    if ! timeout 30 $CLICKHOUSE_CLIENT \
        --query "KILL QUERY WHERE query_id = '$query_id' FORMAT Null" 2>"$err"; then
        echo "KILL QUERY did not return while finish was parked in its drain:"
        cat "$err"
        failed=1
    fi
fi

# Release the drain first, so the reader wakes onto connections that are already drained.
release_all

# The query was killed, so a non-zero client status is the expected outcome here - not an assertion.
wait 2>/dev/null ||:

if [ "$sync_ok" -eq 1 ]; then
    # Positive control: prove the parked thread really was past `tryCancel`, so this cannot pass by
    # never reaching the drain. `finish` and `cancelUnlocked` log different reasons through the same
    # `LOG_TRACE`, so match the whole suffix rather than the prefix they share.
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    cancelled=$($CLICKHOUSE_CLIENT --query "
        SELECT count() > 0 FROM system.text_log
        WHERE query_id = '$query_id' AND event_date >= yesterday()
          AND endsWith(message, 'Cancelling query because enough data has been read')
        SETTINGS max_rows_to_read = 0")
    if [ "$cancelled" != "1" ]; then
        echo "cancellation was not logged, so the drain was never reached"
        failed=1
    fi

    assert_query_gone "$query_id"
fi

# Separate liveness check: the server survived.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

########## Scenario B: cancel arrives before finish announces itself ##########

sync_ok=0
query_id_b="${CLICKHOUSE_TEST_UNIQUE_NAME}_gate_order"
kill_done="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.kill_done"
rm -f "$kill_done" "$kill_done.part"

armed=0
arm "$FP_RECV" && arm "$FP_ENTRY" && arm "$FP_GATE" && arm "$FP_HOLD" && armed=1

# Independent of scenario A's outcome, so one failing scenario cannot hide the other.
if [ "$armed" -eq 1 ]; then
    start_query "$query_id_b"

    # The parked shard's `finish` must be held at the very top of the function, before it publishes
    # itself. Both waits must succeed or the interleaving below was never established.
    sync_ok=1
    for fp in "$FP_RECV" "$FP_ENTRY"; do
        if ! wait_pause "$fp"; then
            failed=1
            sync_ok=0
            # The fixture is already broken and `wait_pause` has already named the failpoint; a second
            # 30 s wait cannot add information, and the runner's budget is finite.
            break
        fi
    done
fi

if [ "$sync_ok" -eq 1 ]; then
    # The `KILL` thread runs `cancel`, which finds no `finish` announced yet and parks holding only
    # the gate. It cannot return until the gate is released, so it is polled in step 3 rather than
    # bounded here.
    # The sentinel carries the client's status, and is renamed into place so that seeing the file
    # implies seeing a complete status: a `KILL` that fails fast returns too, and must not read as
    # one that completed.
    ( timeout 120 $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id_b' FORMAT Null" \
        >/dev/null 2>&1; echo "$?" > "$kill_done.part"; mv "$kill_done.part" "$kill_done" ) &

    if ! wait_pause "$FP_GATE"; then
        echo "cancel never reached the gate"
        failed=1
        sync_ok=0
    fi
fi

if [ "$sync_ok" -eq 1 ]; then
    # `cancel` has now observed "no finish in progress" and has not yet taken `was_cancelled_mutex`;
    # the parked shard's `finish` is one step from announcing itself and taking the same mutex.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_ENTRY"

    # Assertion 1: `finish` must not reach its drain, because the gate `cancel` holds admits it only
    # after `cancel` owns `was_cancelled_mutex`. The drain park is executor-local, so only the parked
    # shard can satisfy this wait.
    timeout 5 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP_HOLD PAUSE" 2>"$err"
    hold_status=$?
    if [ "$hold_status" -eq 0 ]; then
        echo "finish reached its drain while cancel held the gate"
        failed=1
    elif [ "$hold_status" -ne 124 ]; then
        # Only `timeout`'s 124 means the wait ran its course; any other status is a broken rig
        # rather than the property under test, so it must not read as a pass.
        echo "wait for failpoint $FP_HOLD ended with status $hold_status, not the expected timeout:"
        cat "$err"
        failed=1
    fi

    # Assertion 2: releasing the gate must let the `KILL` complete, with the drain park still armed
    # and unreleased. The `KILL` thread is inside the parked shard's `cancel`, so nothing but that
    # executor's own `was_cancelled_mutex` can delay it.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_GATE"
    kill_ok=0
    for _ in {1..100}; do
        if [ -f "$kill_done" ]; then
            kill_ok=1
            break
        fi
        sleep 0.3
    done
    if [ "$kill_ok" -ne 1 ]; then
        echo "KILL QUERY did not return after the cancel gate was released"
        failed=1
    elif [ "$(cat "$kill_done")" != "0" ]; then
        echo "KILL QUERY returned status $(cat "$kill_done") after the cancel gate was released"
        failed=1
    fi
fi

# `finish` never reaches the drain park on fixed code, so this disable is unconditional.
release_all
wait 2>/dev/null ||:

if [ "$sync_ok" -eq 1 ]; then
    assert_query_gone "$query_id_b"
fi

# Negative control: with nothing armed the same query still returns its row.
$CLICKHOUSE_CLIENT --enable_parallel_replicas=0 --async_socket_for_remote=0 \
    --max_block_size=1 --prefer_localhost_replica=0 --max_threads=2 \
    --query "SELECT count() FROM (SELECT x FROM ${CLICKHOUSE_DATABASE}.dist LIMIT 1)"

rm -f "$err" "$err_bg" "$kill_done" "$kill_done.part"

# Separate liveness check: the server survived.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

[ "$failed" -eq 0 ] || exit 1
