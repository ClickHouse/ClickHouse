#!/usr/bin/env bash
# Tags: no-parallel, shard
# Tag no-parallel: uses a global failpoint, which concurrent instances would share - one query would
#   consume another's arming, and the injected cancel would land in an unrelated query.
# Tag shard: uses a two-shards Distributed table.

# Regression test for `RemoteQueryExecutor::cancel` waiting for `finish`'s packet drain. `finish`
# holds `was_cancelled_mutex` across a blocking `receivePacket`, and `ExecutingGraph::cancel` calls
# `cancel` on every processor in turn under `processors_mutex`, so one draining remote source used to
# stall cancellation of the whole pipeline.
#
# The defect is a state, not an interleaving: `cancel` only has to run while a thread is inside
# `finish` holding that mutex. So the failpoint runs `cancel()` inline, on the draining thread
# itself, rather than parking that thread and cancelling from outside - `finish` runs from
# `RemoteSource::work`, and a park inside `IProcessor::work()` would break the contract in
# `src/Processors/IProcessor.h`. 04512 injects the mirror of this, at the reader, on the same grounds.
#
# Without the gate in `cancel` the injected call re-locks a mutex its own thread already holds, so
# neither the drain nor the query ever completes; with it, `cancel` observes a `finish` in progress
# and returns.
#
# `LIMIT 1` closes the initiator's output ports while both shards are still streaming, so `finish` runs
# with a real drain left to do, and `distributed_push_down_limit=0` keeps the shards from applying the
# `LIMIT` themselves and finishing after one granule. What decides whether the drain is still outstanding
# is how many packets that port close leaves behind, and blocks arrive granule-aligned here
# (`max_block_size` clamps up to a granule, it does not split one). The runner randomizes
# `index_granularity` over [1, 65536], so unpinned the fixture leaves anywhere from 100000 packets per
# shard down to two, and the two it drew for the run that failed (43695) let `read()` reach `EndOfStream`
# first. `index_granularity = 256` leaves ~390 per shard on every run. Measured on a two-core-confined
# server: 8 runs in 150 where neither shard reached its drain unpinned, 0 in 150 pinned.
# `enable_parallel_replicas=0` keeps `drain_was_skipped` false, which is what leads into the drain;
# `async_socket_for_remote=0` keeps the fixture on the synchronous read path, as 04512 does.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="remote_query_executor_cancel_in_finish_drain"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.src (x UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS index_granularity = 256;
    INSERT INTO ${CLICKHOUSE_DATABASE}.src SELECT number FROM numbers(100000);
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dist AS ${CLICKHOUSE_DATABASE}.src
        ENGINE = Distributed(test_cluster_two_shards, ${CLICKHOUSE_DATABASE}, src);
"

failed=0
err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

# `enabled` is read before and after the query with no DISABLE in between, so 1 then 0 can only
# happen through a fire - the ONCE failpoint disarms itself. `enabled = 0` alone would be vacuous
# because an un-armed failpoint reads 0 too. It is also the positive control for the interleaving:
# the injection site sits past `tryCancel` and past the `drain_was_skipped` branch, so a fire proves
# `cancel` ran on a thread that held the mutex and was entering the drain. Reading
# `system.fail_points` does not consume a failpoint (03916 regression-tests that);
# `max_rows_to_read = 0` guards against randomisation.
function enabled()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT enabled FROM system.fail_points WHERE name = '$FP' SETTINGS max_rows_to_read = 0"
}

# Fail loudly if the failpoint is not available: a run that proceeds un-armed proves nothing.
if ! $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP" 2>"$err"; then
    echo "cannot arm failpoint $FP:"
    cat "$err"
    failed=1
else
    echo "armed $(enabled)"

    # The count is asserted, not discarded: the suppressed `cancel` must not cost the query its row.
    # Bounded, because the unfixed failure mode is a deadlock rather than an error, and bounded well
    # inside Fast test's 60 s per-test allowance so that on a regression the diagnosis below is what
    # surfaces, not the runner's bare timeout. The query itself returns in under a second here.
    #
    # Retried while the failpoint is still armed: a query whose `finish` calls all returned before the
    # drain has exercised nothing, so passing on it would be vacuous. The granularity pin makes one
    # attempt enough in practice; this keeps a residual miss from reddening the check. It cannot hide a
    # regression, which shows up on the first attempt that does fire.
    attempts=0
    while [ "$attempts" -lt 10 ]; do
        attempts=$((attempts + 1))

        if ! rows=$(timeout 30 $CLICKHOUSE_CLIENT \
            --enable_parallel_replicas=0 --async_socket_for_remote=0 --distributed_push_down_limit=0 \
            --max_block_size=1 --prefer_localhost_replica=0 \
            --query "SELECT count() FROM (SELECT x FROM ${CLICKHOUSE_DATABASE}.dist LIMIT 1)" 2>"$err"); then
            echo "the query did not complete with cancel injected into finish's drain:"
            cat "$err"
            failed=1
            break
        fi

        if [ "$(enabled)" = "0" ]; then
            echo "$rows"
            break
        fi
    done

    if [ "$(enabled)" = "1" ]; then
        echo "no finish reached its drain in $attempts attempts, so cancel was never injected"
        failed=1
    fi

    echo "consumed $(enabled)"
fi

rm -f "$err"

# Separate liveness check: the server survived.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

[ "$failed" -eq 0 ] || exit 1
