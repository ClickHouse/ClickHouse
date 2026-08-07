#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: uses the s3 table function.

# `KILL MUTATION` must stop a mutation whose `IN` subquery is stuck reading an unreachable S3
# endpoint. The read happens while the mutation's plan is still being built (materializing the set for
# primary-key analysis), where cancellation is observable only through the thread's cancellation
# predicate -- which used to be constant `false` for background tasks, so the mutation retried the
# request to its budget and blocked any later DROP/DETACH of the table behind it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mut (id UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mut SELECT number FROM numbers(1000)"

# Nothing listens on this port, so the read never completes on its own.
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_mut DELETE WHERE id IN (
        SELECT * FROM s3('http://localhost:19999/dummy.parquet', 'NOSIGN', 'One')
    ) SETTINGS mutations_sync = 0"

# Wait for the mutation to actually start before killing it, otherwise the kill races the scheduler
# and the test would pass without ever exercising the cancellation.
for _ in {1..150}; do
    started=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_mut'")
    [[ "$started" -ge 1 ]] && break
    sleep 0.2
done
echo "mutation started: $([[ "$started" -ge 1 ]] && echo 1 || echo 0)"

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_mut' FORMAT Null"

# The oracle is the task LEAVING system.merges, not the KILL statement returning: the KILL returns
# immediately even when the mutation keeps retrying, which is precisely the bug. No wall-clock
# assertion -- only that it stops well inside the retry budget it used to exhaust.
gone=0
for _ in {1..300}; do
    running=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_mut'")
    if [[ "$running" -eq 0 ]]; then
        gone=1
        break
    fi
    sleep 0.2
done
echo "mutation stopped: $gone"

# The table must now be droppable: before the fix the in-flight task held it and the DROP blocked in
# MergeTreeBackgroundExecutor::removeTasksCorrespondingToStorage, which is what the stress-test hung
# check reported. Bounded so a regression is reported as a diff rather than as a test timeout.
timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_mut SYNC" >/dev/null 2>&1
echo "dropped: $?"
