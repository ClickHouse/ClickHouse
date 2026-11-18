#!/usr/bin/env bash
# Tags: no-parallel, shard
# Tag no-parallel: waits on a server-global PAUSEABLE failpoint, so a concurrent instance would
#   consume this one's pause and release the writing job early.
# Tag shard: inserts into a Distributed table.

# Regression test for cancellation of a synchronous distributed INSERT. `DistributedSink::onCancel`
# used to take the mutex that `writeSync` holds across its wait for the writing jobs, and then wait
# for the pool itself before cancelling any executor, so cancelling such an INSERT waited for it to
# finish instead of interrupting it. `KILL QUERY` therefore blocked, and so did shutdown, which
# cancels every running query on the main thread.
#
# The failpoint replaces timing: it parks the single writing job before it pushes, which is exactly
# the state in which `writeSync` holds the mutex inside its wait. A `KILL QUERY` issued while the
# job is parked must return; before the fix it could only return once the job was released.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="distributed_sink_pause_before_push"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dst" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dst;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dist;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dist AS ${CLICKHOUSE_DATABASE}.dst
        ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, dst);
"

failed=0
err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

# A run that proceeds un-armed proves nothing, so stop instead of asserting on it.
if ! $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP" 2>"$err"; then
    echo "cannot arm failpoint $FP:"
    cat "$err"
    exit 1
fi

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_insert"
# prefer_localhost_replica=0 turns the single shard into one remote writing job, so exactly one job
# reaches the failpoint and the pool is provably waiting for it.
$CLICKHOUSE_CLIENT \
    --query_id "$query_id" \
    --distributed_foreground_insert 1 --prefer_localhost_replica 0 \
    --query "INSERT INTO ${CLICKHOUSE_DATABASE}.dist SELECT number FROM numbers(10)" 2>"$err" &
INS_PID=$!

if ! timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE" 2>"$err"; then
    echo "wait for failpoint $FP failed:"
    cat "$err"
    failed=1
fi

kill_out=$(timeout 60 $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' ASYNC" 2>"$err")
kill_rc=$?
if [ "$kill_rc" -ne 0 ]; then
    echo "kill did not return while the write was parked (exit $kill_rc)"
    cat "$err"
    failed=1
elif [ -z "$kill_out" ]; then
    # Nothing was cancelled, so the check above passed without exercising anything.
    echo "kill matched no query"
    failed=1
fi

# Release the parked job. The INSERT is expected to fail, and which error surfaces depends on where
# the cancellation is observed, so its exit status is not an assertion.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait "$INS_PID" 2>/dev/null ||:

rm -f "$err"

# A cancelled writing job must not push its block. `prefer_localhost_replica=0` makes the shard-side
# INSERT a separate query, so the bytes it received are an external witness: none if the job stopped
# before pushing, the block's worth if it pushed anyway. The client's own error does not distinguish
# the two, because the cancelled query reports QUERY_WAS_CANCELLED either way.
# The shard-side query runs on the connection the cluster provides, so its own database is not this
# test's; it is identified by initial_query_id, which carries this test's unique name and therefore
# cannot match another test's rows. The initiator's row is checked in this database as a control that
# the insert under test is the one being measured.
initiator_rows=0
shard_rows=0
for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    initiator_rows=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type != 'QueryStart'")
    shard_rows=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.query_log
        WHERE initial_query_id = '$query_id' AND is_initial_query = 0 AND type != 'QueryStart'")
    [ "$initiator_rows" -gt 0 ] && [ "$shard_rows" -gt 0 ] && break
    sleep 1
done

if [ "$initiator_rows" -eq 0 ] || [ "$shard_rows" -eq 0 ]; then
    # Without both rows the byte check below would hold vacuously, so this is a failure of its own.
    echo "query_log is missing the insert (initiator rows $initiator_rows, shard rows $shard_rows)"
    failed=1
else
    received=$($CLICKHOUSE_CLIENT --query "
        SELECT sum(ProfileEvents['NetworkReceiveBytes']) FROM system.query_log
        WHERE initial_query_id = '$query_id' AND is_initial_query = 0 AND type != 'QueryStart'")
    if [ "$received" != "0" ]; then
        echo "cancelled writing job still sent its block to the shard ($received bytes)"
        failed=1
    fi
fi

# Separate liveness check: the server survived the cancelled insert.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

[ "$failed" -eq 0 ] || exit 1
