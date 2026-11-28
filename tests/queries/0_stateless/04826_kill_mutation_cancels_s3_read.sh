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

# Nothing listens on this port, so the read never completes on its own. `enable_parallel_replicas = 0`
# keeps the read under test inside the background mutation: with parallel replicas `s3` resolves to the
# cluster engine, whose constructor probes the endpoint while `ALTER` is still being analysed, so the
# statement blocks instead of queueing a mutation.
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_mut DELETE WHERE id IN (
        SELECT * FROM s3('http://localhost:19999/dummy.parquet', 'NOSIGN', 'One')
    ) SETTINGS mutations_sync = 0, enable_parallel_replicas = 0"

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
# happened to produce. A cancellation the read reports as an ordinary request failure instead would
# end the mutation in a misleading S3_ERROR, so error 236 and not 499 is the assertion.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
echo "cancelled, not S3 error: $($CLICKHOUSE_CLIENT -q "
    SELECT countIf(error = 236) > 0 AND countIf(error = 499) = 0
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_mut'
      AND event_type = 'MutatePart' AND error != 0")"

# The table must now be droppable: before the fix the in-flight task still held it, so the DROP waited
# for the read to exhaust its retries, which is what the stress-test hung check reported. Bounded so a
# regression is reported as a diff rather than as a test timeout.
timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_mut SYNC" >/dev/null 2>&1
echo "dropped: $?"

# The second cancellation source: stopping merges must reach the read as well, so that shutting a
# table (or the server) down does not wait out the retry budget either. `KILL MUTATION` cancels only
# the one mutation, so without this arm nothing covers the per-table stop.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_stop (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0, auto_statistics_types = ''"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_stop SELECT number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_stop DELETE WHERE id IN (
        SELECT * FROM s3('http://localhost:19999/dummy.parquet', 'NOSIGN', 'One')
    ) SETTINGS mutations_sync = 0, enable_parallel_replicas = 0"
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
#
# Exactly ONE read here. The flaky check runs this test in as many workers as the runner has cores,
# and they share the server's background mutation slots, so a premise that needs several of this
# run's own mutations resident at the same instant is not satisfiable in that job.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_toggle (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0, auto_statistics_types = ''"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_toggle SELECT number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_toggle DELETE WHERE id IN (
        SELECT * FROM s3('http://localhost:19999/dummy.parquet', 'NOSIGN', 'One')
    ) SETTINGS mutations_sync = 0, enable_parallel_replicas = 0"
for _ in {1..150}; do
    started=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.merges
        WHERE database = currentDatabase() AND table = 't_toggle'")
    [[ "$started" -ge 1 ]] && break
    sleep 0.2
done
echo "toggle: mutation started: $([[ "$started" -ge 1 ]] && echo 1 || echo 0)"

# An entry in system.merges only means the task is running: the subquery's S3 client is built later,
# and under a sanitizer that took 16s, so a toggle can be over before the read makes its first
# attempt and cancel nothing. Each round leaves the mutation schedulable, so it is simply retried.
# No round is STARTED past the deadline below, which leaves one whole round plus the teardown
# inside the flaky check's per-test limit of 180s however slow the arms above were.
cancelled=0
toggle_deadline=$((SECONDS + 70))
[[ "$toggle_deadline" -gt 125 ]] && toggle_deadline=125
while [[ "$cancelled" -eq 0 && "$SECONDS" -lt "$toggle_deadline" ]]; do
    # The read tests cancellation once per retry attempt, 5s apart once its backoff has saturated.
    # The settle both waits for that and is drawn over a whole attempt period: a narrower draw
    # resonates with it, because the restart after a cancelled attempt re-creates the same phase.
    settle=$((9000 + RANDOM % 5100))
    sleep "$(printf '%d.%03d' $((settle / 1000)) $((settle % 1000)))"
    # One call: the window is a server-side sleep plus two round trips, so client startup cannot
    # stretch it, and the fail times bracket it. It has to hold the blocker cancelled long enough for
    # a poller to observe it, and still end well inside the read's own attempt cadence: an attempt
    # that fell INSIDE the window read the blocker itself, which proves nothing, so that round is
    # spent on a fresh draw rather than on the poll below.
    mapfile -t stamps < <($CLICKHOUSE_CLIENT -q "
        SELECT max(toUnixTimestamp(latest_fail_time)) FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_toggle';
        SYSTEM STOP MERGES t_toggle;
        SELECT sleep(0.5) FORMAT Null;
        SYSTEM START MERGES t_toggle;
        SELECT max(toUnixTimestamp(latest_fail_time)) FROM system.mutations
        WHERE database = currentDatabase() AND table = 't_toggle'")
    [[ "${#stamps[@]}" -eq 2 && "${stamps[0]}" == "${stamps[1]}" ]] || continue
    # A released blocker answers every later poll with "not cancelled", so a cancellation recorded
    # after the release can only come from one that was persisted. An earlier one would only show
    # the read reading the blocker itself, which the arm above already covers.
    seen_deadline=$((SECONDS + 10))
    while [[ "$SECONDS" -lt "$seen_deadline" ]]; do
        seen=$($CLICKHOUSE_CLIENT -q "
            SELECT countIf(latest_fail_reason LIKE '%Cancelled mutating parts%'
                           AND toUnixTimestamp(latest_fail_time) > ${stamps[1]})
            FROM system.mutations
            WHERE database = currentDatabase() AND table = 't_toggle'")
        if [[ "${seen:-0}" -ge 1 ]]; then
            cancelled=1
            break
        fi
        sleep 0.5
    done
done
echo "toggle stop/start cancels the read: $cancelled"
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_toggle' FORMAT Null"
timeout 60 $CLICKHOUSE_CLIENT -q "DROP TABLE t_toggle SYNC" >/dev/null 2>&1
