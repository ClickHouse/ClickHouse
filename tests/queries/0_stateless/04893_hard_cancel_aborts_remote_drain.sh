#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, shard
# Tag no-parallel: uses global PAUSEABLE failpoints, which concurrent instances would share.
# Tag shard: uses a two-shards Distributed table.

# Regression test for the hard-cancellation upgrade on the common `LIMIT` carrier: when the consumer
# closes the output port, `RemoteSource` drains the remaining packets through
# `RemoteQueryExecutor::finish` without any cancellation being issued, so the soft `PartialResult`
# reason has to be published by `RemoteSource` itself. Otherwise a `KILL QUERY` arriving while that
# drain is in flight becomes the first recorded cancellation reason, never reaches `abortDrain`, and
# the user has to wait for the replicas to reach end-of-stream.
#
# The interleaving is fixed by three failpoints instead of timing (a superset of the sequence of
# `04512_distributed_limit_no_more_packets_race`):
#   fp1 parks the synchronous reader inside `read`, before its first `receivePacket`;
#   fp2 parks `finish` once it has delegated the drain to that parked reader - waiting for it
#       before releasing fp1 guarantees the reader wakes up with the drain already delegated, so
#       it enters the drain loop with its shard's data block as the first packet (at least the
#       trailing `EndOfStream` is still unread, so the loop is guaranteed to iterate). Without
#       this step, releasing fp1 too early lets both shards finish through the normal read path
#       and nothing ever reaches fp3 (the `SYSTEM WAIT FAILPOINT` below then never returns);
#   fp3 parks the drain loop right before it checks the abort flag - the `KILL QUERY` is issued
#       there, so it is guaranteed to arrive while the drain is in flight (`KILL ... ASYNC` returns
#       only after `PipelineExecutor::cancel` has synchronously cancelled every processor).
# Releasing fp3 then makes the loop leave through `drain_should_stop`, which is what the `text_log`
# assertion checks: without the fix the loop keeps draining and the message is never written.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP_RECV="remote_query_executor_receive_packet_pause"
FP_FINISH="remote_query_executor_finish_drain_pause"
FP_DRAIN="remote_query_executor_drain_packet_pause"

# A previous run of this test (or of `04512_distributed_limit_no_more_packets_race`, which shares
# these failpoints) may have been killed by the harness before its `cleanup` trap ran, leaving a
# failpoint armed. A leftover armed failpoint pauses a thread of this run's query at a point the
# synchronization below does not expect, so start from a clean slate.
for fp in "$FP_RECV" "$FP_FINISH" "$FP_DRAIN"; do
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
    for fp in "$FP_RECV" "$FP_FINISH" "$FP_DRAIN"; do
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $fp" 2>/dev/null ||:
    done
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src" 2>/dev/null ||:
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

function wait_pause()
{
    if ! $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $1 PAUSE" 2>"$err"; then
        echo "wait for failpoint $1 failed:"
        cat "$err"
        failed=1
        return 1
    fi
}

# `$$` keeps the identifier unique across re-runs against the same server, so the `text_log`
# assertion below cannot match a row left by a previous run.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_$$_drain_abort"

if arm "$FP_RECV" && arm "$FP_FINISH" && arm "$FP_DRAIN"; then
    # `LIMIT 1` without `ORDER BY`: the sibling shard delivers a row and closes the output port,
    # so `finish` drains the parked source's executor. `async_socket_for_remote=0` picks the
    # synchronous read path the delegation happens on.
    $CLICKHOUSE_CLIENT \
        --query_id "$query_id" \
        --enable_parallel_replicas=0 --async_socket_for_remote=0 \
        --max_block_size=1 --prefer_localhost_replica=0 \
        --query "SELECT x FROM ${CLICKHOUSE_DATABASE}.dist LIMIT 1 FORMAT Null" 2>"$err" &
    QPID=$!

    if wait_pause "$FP_RECV" && wait_pause "$FP_FINISH"; then
        # `finish` has delegated the drain to the parked reader (fp2 pauses only in that branch,
        # and only for the executor whose reader is parked at fp1). Release `finish` first - it
        # pauses under `was_cancelled_mutex`, which the woken reader needs - then the reader.
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_FINISH"
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_RECV"

        if wait_pause "$FP_DRAIN"; then
            killed=$($CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' ASYNC FORMAT TSV" | wc -l)
            if [ "$killed" -eq 0 ]; then
                echo "the query was not found by KILL QUERY"
                failed=1
            fi
        fi
    fi

    for fp in "$FP_RECV" "$FP_FINISH" "$FP_DRAIN"; do
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $fp"
    done

    # The query is expected to be cancelled, so its exit status is not an assertion here.
    wait "$QPID" 2>/dev/null ||:

    if [ "$failed" -eq 0 ]; then
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
        aborted=$($CLICKHOUSE_CLIENT --query "
            SELECT count() > 0 FROM system.text_log
            WHERE query_id = '$query_id' AND event_date >= yesterday()
              AND message LIKE '%Drain of the remote connections was aborted by a hard cancellation%'
            SETTINGS max_rows_to_read = 0")
        if [ "$aborted" != "1" ]; then
            echo "the drain was not aborted by the hard cancellation"
            failed=1
        fi
    fi
fi

rm -f "$err"

# Separate liveness check: the server survived the cancellation.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

[ "$failed" -eq 0 ] || exit 1
