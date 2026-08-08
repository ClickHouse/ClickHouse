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

# The pool pin makes the mutation schedulable regardless of what else this run has queued, and
# `auto_statistics_types = ''` keeps the set from being built via the statistics estimation path, so the
# read under test is the key-analysis build. Both siblings of this family pin the same pair.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_mut (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0, auto_statistics_types = ''"
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

# It must stop *because it was cancelled*, not with whatever S3/network error the last attempt
# happened to produce. That distinction is what covers the throwing half of the cancellation
# predicate: `Client::HeadObject` reports a killed read through `CurrentThread::checkIfNotCancelled`,
# and with only the boolean half installed the mutation ends in a misleading `S3_ERROR` instead.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
echo "cancelled, not S3 error: $($CLICKHOUSE_CLIENT -q "
    SELECT countIf(error = 236) > 0 AND countIf(error = 499) = 0
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_mut'
      AND event_type = 'MutatePart' AND error != 0")"

# The table must now be droppable: before the fix the in-flight task held it and the DROP blocked in
# MergeTreeBackgroundExecutor::removeTasksCorrespondingToStorage, which is what the stress-test hung
# check reported. Bounded so a regression is reported as a diff rather than as a test timeout.
timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_mut SYNC" >/dev/null 2>&1
echo "dropped: $?"

# The second cancellation source: stopping merges must reach the read as well, so that shutting a
# table (or the server) down does not wait out the retry budget either. `KILL MUTATION` only sets the
# merge-list entry's flag, so without this arm the `merges_blocker` half of the predicate is unpinned.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_stop (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0, auto_statistics_types = ''"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_stop SELECT number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_stop DELETE WHERE id IN (
        SELECT * FROM s3('http://localhost:19999/dummy.parquet', 'NOSIGN', 'One')
    ) SETTINGS mutations_sync = 0"
for _ in {1..150}; do
    started=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_stop'")
    [[ "$started" -ge 1 ]] && break
    sleep 0.2
done
echo "stop merges: mutation started: $([[ "$started" -ge 1 ]] && echo 1 || echo 0)"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_stop"
stopped=0
for _ in {1..300}; do
    running=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_stop'")
    if [[ "$running" -eq 0 ]]; then
        stopped=1
        break
    fi
    sleep 0.2
done
echo "stop merges cancels the read: $stopped"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_stop"
timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_stop SYNC" >/dev/null 2>&1

# The blocker above stays cancelled until the task exits, so it does not cover a blocker that is
# released again while the read is still retrying -- which ordinary DDL does on every scope exit
# (TRUNCATE, DROP/DETACH PARTITION, REPLACE PARTITION all take a scoped lock). The read and the
# interactive-cancel callback poll the predicate independently, so a detected cancellation must be
# persisted rather than re-read, exactly as the merge path already does.
#
# The oracle here is the attempt REPORTING cancellation, not the task leaving system.merges: a
# released blocker leaves the mutation schedulable, so the entry legitimately reappears and the
# arm above's oracle is unsatisfiable for this shape.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_toggle (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0, auto_statistics_types = ''"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_toggle SELECT number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_toggle DELETE WHERE id IN (
        SELECT * FROM s3('http://localhost:19999/dummy.parquet', 'NOSIGN', 'One')
    ) SETTINGS mutations_sync = 0"
for _ in {1..150}; do
    started=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_toggle'")
    [[ "$started" -ge 1 ]] && break
    sleep 0.2
done
echo "toggle: mutation started: $([[ "$started" -ge 1 ]] && echo 1 || echo 0)"

# Let the retry loop reach its steady cadence, so the release below lands between two of the
# read's own checks -- with a fresh attempt the read rechecks within milliseconds and the window
# never opens.
sleep 12
# The blocker must stay cancelled long enough for a poller to see it -- back to back the window is
# shorter than any poll interval, so nothing observes it and there is nothing to latch. It is then
# released while the read is still between attempts, which is the ordering under test.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_toggle"
sleep 0.5
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_toggle"
cancelled=0
for _ in {1..300}; do
    seen=$($CLICKHOUSE_CLIENT -q "
        SELECT countIf(latest_fail_reason LIKE '%Cancelled mutating parts%')
        FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_toggle'")
    if [[ "$seen" -ge 1 ]]; then
        cancelled=1
        break
    fi
    sleep 0.2
done
echo "toggle stop/start cancels the read: $cancelled"
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_toggle' FORMAT Null"
timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_toggle SYNC" >/dev/null 2>&1
