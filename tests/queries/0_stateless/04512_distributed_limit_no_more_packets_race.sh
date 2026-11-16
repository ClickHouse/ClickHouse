#!/usr/bin/env bash
# Tags: no-parallel, shard
# Tag no-parallel: uses a global failpoint, which concurrent instances would share - one query
#   would consume another's arming, and the injected cancel would land in an unrelated query.
# Tag shard: uses a two-shards Distributed table.

# Regression test for a race in the synchronous distributed-read path: `RemoteQueryExecutor::read`
# checks `was_cancelled`, releases the mutex, then calls `receivePacket`, while another pipeline
# thread's `onUpdatePorts` -> `finish` cancels and drains the same connections. The reader then used
# to throw LOGICAL_ERROR "No more packets are available.".
#
# The defect is a state, not an interleaving: the reader only has to reach `receivePacket` with the
# connections cancelled and drained. So the failpoint runs that same `finish()` inline, on the
# reading thread, instead of parking it there and cancelling from outside - a park inside
# `IProcessor::work()` would break the contract in `src/Processors/IProcessor.h`.
#
# The query runs under both `use_hedged_requests` values because the fix touches both
# `HedgedConnections` and `MultiplexedConnections`; `async_socket_for_remote=0` picks the sync path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="remote_query_executor_cancel_and_drain_in_receive_window"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${CLICKHOUSE_DATABASE}.src SELECT number FROM numbers(1000);
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dist AS ${CLICKHOUSE_DATABASE}.src
        ENGINE = Distributed(test_cluster_two_shards, ${CLICKHOUSE_DATABASE}, src);
"

failed=0
err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

# `enabled` is read before and after the query with no DISABLE in between, so 1 then 0 can only
# happen through a fire - the ONCE failpoint disarms itself. `enabled = 0` alone would be vacuous
# because an un-armed failpoint reads 0 too. Reading `system.fail_points` does not consume a
# failpoint (03916 regression-tests that); `max_rows_to_read = 0` guards against randomisation.
function enabled()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT enabled FROM system.fail_points WHERE name = '$FP' SETTINGS max_rows_to_read = 0"
}

for use_hedged_requests in 0 1; do
    # Fail loudly if the failpoint is not available: a run that proceeds un-armed proves nothing.
    if ! $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP" 2>"$err"; then
        echo "cannot arm failpoint $FP:"
        cat "$err"
        failed=1
        break
    fi
    echo "armed_$use_hedged_requests $(enabled)"

    # No LIMIT: with the cancel injected in place, a LIMIT could satisfy itself and cancel both
    # executors before either reader entered the loop, so the failpoint would not fire at all.
    # Parallel replicas force `use_hedged_requests` off, which would leave HedgedConnections
    # untested; `prefer_localhost_replica=0` makes both shards go through RemoteQueryExecutor.
    if ! $CLICKHOUSE_CLIENT \
        --use_hedged_requests="$use_hedged_requests" --enable_parallel_replicas=0 \
        --async_socket_for_remote=0 --prefer_localhost_replica=0 \
        --query "SELECT x FROM ${CLICKHOUSE_DATABASE}.dist FORMAT Null" 2>"$err"
    then
        # Without the fix a debug build aborts the server on the LOGICAL_ERROR and a release build
        # returns it to the client, so this covers both.
        echo "query under use_hedged_requests=$use_hedged_requests failed:"
        cat "$err"
        failed=1
    fi

    echo "consumed_$use_hedged_requests $(enabled)"
done

rm -f "$err"

# Separate liveness check: the server survived both modes.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

[ "$failed" -eq 0 ] || exit 1
