#!/usr/bin/env bash
# Tags: no-parallel, shard
# Tag no-parallel: the failpoints are server-global and this test *waits* on them, so a concurrent
#   instance could satisfy this instance's `SYSTEM WAIT FAILPOINT ... PAUSE` from its own executor.
#   `PAUSEABLE_ONCE` bounds parks per arming, not across concurrent instances.
# Tag shard: uses a two-shards Distributed table.

# Regression test for `RemoteQueryExecutor::cancel` waiting for `finish`'s packet drain. `finish`
# holds `was_cancelled_mutex` across an unbounded blocking `receivePacket`, and
# `ExecutingGraph::cancel` calls `cancel` on every processor in turn under `processors_mutex`, so one
# draining remote source used to stall cancellation of the whole pipeline.
#
# Two failpoints replace timing: fp1 parks one shard's reader before it consumes a packet, so that
# shard's query is still pending when `LIMIT 1` closes the output ports; fp2 then parks `finish` at
# the start of its drain, holding `was_cancelled_mutex`. `KILL QUERY` has to return while fp2 is
# still held. That is an ordering assertion, not a duration one: while the park is held the unfixed
# latency is unbounded, so the bound below only has to exceed a normal `KILL QUERY` round-trip.
# `async_socket_for_remote=0` picks the synchronous read path, which is where fp1 lives; the drain
# `fp2` parks is synchronous either way.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP_RECV="remote_query_executor_receive_packet_pause"
FP_HOLD="remote_query_executor_finish_drain_hold"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_HOLD" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_RECV" 2>/dev/null ||:
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

arm "$FP_RECV" && arm "$FP_HOLD"

if [ "$failed" -eq 0 ]; then
    # `LIMIT 1` without `ORDER BY`: the shard that is not parked delivers a row and closes the output
    # ports, so `onUpdatePorts` calls `finish` on the parked shard's executor and reaches the drain.
    # `enable_parallel_replicas=0` keeps `drain_was_skipped` false, which is what leads into the drain.
    $CLICKHOUSE_CLIENT \
        --query_id "$query_id" \
        --enable_parallel_replicas=0 --async_socket_for_remote=0 \
        --max_block_size=1 --prefer_localhost_replica=0 \
        --query "SELECT x FROM ${CLICKHOUSE_DATABASE}.dist LIMIT 1 FORMAT Null" 2>"$err" &
    QPID=$!

    # Without both parks the interleaving never happened and the assertion below would be vacuous.
    sync_ok=1
    for fp in "$FP_RECV" "$FP_HOLD"; do
        if ! $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $fp PAUSE" 2>"$err"; then
            echo "wait for failpoint $fp failed:"
            cat "$err"
            failed=1
            sync_ok=0
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
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_HOLD"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_RECV"

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

    # The killed query must actually be gone once the parks are released.
    gone=0
    for _ in {1..100}; do
        alive=$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.processes WHERE query_id = '$query_id'")
        if [ "$alive" = "0" ]; then
            gone=1
            break
        fi
        sleep 0.3
    done
    if [ "$gone" -ne 1 ]; then
        echo "query is still running after the failpoints were released"
        failed=1
    fi
fi

rm -f "$err"

# Separate liveness check: the server survived.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

[ "$failed" -eq 0 ] || exit 1
