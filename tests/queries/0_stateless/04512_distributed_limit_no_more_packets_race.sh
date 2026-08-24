#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, shard
# Tag no-parallel: uses global PAUSEABLE failpoints, which concurrent instances would share.
# Tag shard: uses a two-shards Distributed table.

# Regression test for a race in the synchronous distributed-read path: `RemoteQueryExecutor::read`
# checks `was_cancelled`, releases the mutex, then calls `receivePacket`, while a LIMIT closing the
# output port makes another thread cancel and drain the same connections. The reader then used to
# throw LOGICAL_ERROR "No more packets are available.".
#
# Two failpoints replace timing: fp1 parks the reader in that window, fp2 parks the same executor's
# `finish` once it has marked the query cancelled and delegated the drain to the parked reader, so
# the reader is only released after the cancel the fix depends on. The query runs under both
# `use_hedged_requests` values because the fix touches
# `HedgedConnections` and `MultiplexedConnections`; `async_socket_for_remote=0` picks the sync path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP_RECV="remote_query_executor_receive_packet_pause"
FP_DRAIN="remote_query_executor_finish_drain_pause"

# A previous run of this test (or of `04893_hard_cancel_aborts_remote_drain`, which shares these
# failpoints and additionally arms `remote_query_executor_drain_packet_pause`) may have been killed
# by the harness before its `cleanup` trap ran, leaving a failpoint armed. A leftover armed
# `remote_query_executor_drain_packet_pause` is fatal here: this test's drain loop would pause on
# it and this test never disables it, so the query would hang. Start from a clean slate.
for fp in "$FP_RECV" "$FP_DRAIN" "remote_query_executor_drain_packet_pause"; do
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $fp" 2>/dev/null ||:
done
# A leftover query of such a killed run could also still be alive and would consume the pauses of
# the failpoints armed below (they are PAUSEABLE_ONCE, so the first thread through takes the
# pause), breaking the synchronization. Wait for any such query to finish before arming.
$CLICKHOUSE_CLIENT --query "
    KILL QUERY WHERE user = currentUser()
        AND (query_id LIKE '04512_distributed_limit_no_more_packets_race%'
             OR query_id LIKE '04893_hard_cancel_aborts_remote_drain%')
    SYNC FORMAT Null" 2>/dev/null ||:

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_RECV" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_DRAIN" 2>/dev/null ||:
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
err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

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

for use_hedged_requests in 0 1; do
    arm "$FP_RECV" || break
    arm "$FP_DRAIN" || break

    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${use_hedged_requests}"
    # LIMIT 1 without ORDER BY: the sibling shard delivers a row and closes the output port,
    # triggering `finish` to drain the parked source's executor.
    # Parallel replicas force `use_hedged_requests` off, which would run both iterations
    # against MultiplexedConnections and leave the HedgedConnections branch untested.
    $CLICKHOUSE_CLIENT \
        --query_id "$query_id" \
        --use_hedged_requests="$use_hedged_requests" --enable_parallel_replicas=0 \
        --async_socket_for_remote=0 --max_block_size=1 --prefer_localhost_replica=0 \
        --query "SELECT x FROM ${CLICKHOUSE_DATABASE}.dist LIMIT 1 FORMAT Null" 2>"$err" &
    QPID=$!

    # A failed wait would leave the interleaving unsynchronized, and the plain LIMIT query could
    # then succeed and log the cancellation, so check both.
    sync_ok=1
    for fp in "$FP_RECV" "$FP_DRAIN"; do
        if ! $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $fp PAUSE" 2>"$err"; then
            echo "wait for failpoint $fp failed:"
            cat "$err"
            failed=1
            sync_ok=0
        fi
    done

    # Release the reader onto the drained, cancelled connections, then release `finish`.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_RECV"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_DRAIN"

    # The query must succeed: without the fix a debug build aborts the server, while a release
    # build returns the LOGICAL_ERROR to the client, which the exit status below turns into a
    # failure independently of the output diff (a bare "if ! wait" is not itself an assertion).
    if ! wait "$QPID"; then
        echo "query under use_hedged_requests=$use_hedged_requests failed:"
        cat "$err"
        failed=1
    fi

    # Positive control: prove the cancel path this test exists for actually ran, so the query
    # cannot pass by never reaching it. Only meaningful when the synchronization above held.
    [ "$sync_ok" -eq 1 ] || continue
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    cancelled=$($CLICKHOUSE_CLIENT --query "
        SELECT count() > 0 FROM system.text_log
        WHERE query_id = '$query_id' AND event_date >= yesterday()
          AND message LIKE '%Cancelling query because enough data has been read%'
        SETTINGS max_rows_to_read = 0")
    if [ "$cancelled" != "1" ]; then
        echo "cancellation was not logged for use_hedged_requests=$use_hedged_requests"
        failed=1
    fi
done

rm -f "$err"

# Separate liveness check: the server survived both modes.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

[ "$failed" -eq 0 ] || exit 1
