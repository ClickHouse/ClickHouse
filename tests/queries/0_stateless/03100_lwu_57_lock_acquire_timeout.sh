#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel, no-fasttest
# no-replicated-database - path in zookeeper differs with replicated database
# no-parallel: the `completed_pipeline_pause_before_teardown` and `patch_parts_lock_pause_before_cas`
#   failpoints are server-global, so a concurrent test would clear them while this one waits.

# A reduced check of the lightweight update lock: `lock_acquire_timeout` and cancellation bound the
# wait in both Keeper modes and in both modes of a plain `MergeTree`, a wait spanning several chunks
# registers its watch once and is interrupted between chunks, and losing the compare-and-swap on the
# `in_progress` directory retries once instead of spinning on Keeper. Only one cheap arm is kept per
# wait path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# The log tables outlive the tables this test drops, and `clickhouse-test --database` reuses one
# database for every test, so tagging by database alone would let one run match another's rows.
run_id="lwu57-$CLICKHOUSE_DATABASE-$RANDOM$RANDOM"

FP=completed_pipeline_pause_before_teardown
# Only a query whose id starts with this can park at the failpoint, so unrelated pipelines cannot
# consume the one-shot arm.
QID_PREFIX=completed_pipeline_pause_failpoint_

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT patch_parts_lock_pause_before_cas" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_timeout_sync SYNC; DROP TABLE IF EXISTS t_lwu_timeout_auto SYNC; DROP TABLE IF EXISTS t_lwu_cas SYNC; DROP TABLE IF EXISTS t_lwu_plain_sync SYNC; DROP TABLE IF EXISTS t_lwu_plain_auto SYNC" 2>/dev/null || true
}
trap cleanup EXIT

# A holder of the lightweight update lock makes a conflicting update wait for it. In 'auto' mode a
# conflict requires one update to READ the column the other WRITES
# (UpdateAffectedColumns::hasConflict), hence the first update writes `s` and the second reads it.

# Blocks until an update owns the lightweight update lock on $1. Counts the CHILDREN of a node that
# the table always has, so the count is zero until a holder takes the lock and drops back to zero
# when it releases: 'sync' takes a single `lock` node, 'auto' creates one `in_progress/update-*`
# child per update. A history-based wait would instead match an earlier holder of the same table.
function wait_for_lock_held()
{
    local table_name=$1
    local mode=$2

    local updates_path="/zookeeper/$CLICKHOUSE_DATABASE/$table_name/lightweight_updates"
    local condition="path = '$updates_path/in_progress' AND startsWith(name, 'update-')"
    if [[ "$mode" == "sync" ]]
    then
        condition="path = '$updates_path' AND name = 'lock'"
    fi

    for _ in {0..300}
    do
        sleep 0.1
        if [[ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.zookeeper WHERE $condition")" -gt 0 ]]
        then
            return 0
        fi
    done

    echo "Failed to wait for a $mode holder of the lightweight update lock on $table_name" >&2
    exit 2
}

# Starts an update that takes the lightweight update lock, then parks on the query thread with its
# pipeline finished but not yet torn down, holding the lock until release_holder is called. The hold
# does not begin expiring before the waiter starts, so how long a waiter blocks is chosen by this
# test rather than raced against a fixed sleep.
function start_parked_holder()
{
    local table_name=$1
    local mode=$2
    local in_keeper=${3:-1}

    holder_qid="${QID_PREFIX}${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

    # Everything below reads the armed state as evidence, so an unarmed run would be vacuous.
    if [[ "$($CLICKHOUSE_CLIENT --query "SELECT enabled FROM system.fail_points WHERE name = '$FP'")" != 1 ]]
    then
        echo "Failed to arm the pause for a $mode holder on $table_name" >&2
        exit 2
    fi

    $CLICKHOUSE_CLIENT --query_id "$holder_qid" --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET s = 'xx' WHERE id = 2
        SETTINGS update_parallel_mode = '$mode';
    " &

    if ! $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
    then
        echo "Failed to park a $mode holder of the lightweight update lock on $table_name" >&2
        exit 2
    fi

    # The wait returns at once when nothing is parked. The failpoint is one-shot and only a query
    # whose id carries the prefix can consume it, so a zero here is this holder having parked.
    if [[ "$($CLICKHOUSE_CLIENT --query "SELECT enabled FROM system.fail_points WHERE name = '$FP'")" != 0 ]]
    then
        echo "No prefixed query parked for a $mode holder on $table_name" >&2
        exit 2
    fi

    # The lock is taken before the pipeline runs and released only when the pipeline is torn down, so
    # it must be held at the pause.
    if [[ "$in_keeper" == "1" ]]
    then
        wait_for_lock_held "$table_name" "$mode"
    fi
}

# Lets the parked holder finish and release the lock. Callers wait for the background jobs they
# started themselves, so that a waiter can be waited for separately from the holder.
function release_holder()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
}

# Blocks until the query tagged $1 is inside the wait for the lock rather than about to enter it.
# Waits for the watch it registers before waiting, which is set only after an attempt has come back
# saying the lock is taken -- the try counter alone is incremented before that attempt is even sent.
# Where a query got to is a fact a slow runner cannot change, unlike how long it has been waiting.
function wait_for_blocked_on_lock()
{
    local query_id=$1

    for _ in {0..600}
    do
        sleep 0.1
        local watches
        watches=$($CLICKHOUSE_CLIENT --query "
            SYSTEM FLUSH LOGS zookeeper_log;
            SELECT count() FROM system.zookeeper_log
            WHERE type = 'Request' AND has_watch AND query_id = '$query_id'
              AND path LIKE '%/lightweight_updates%'
        ")

        if [[ -n "$watches" && "$watches" -gt 0 ]]
        then
            return 0
        fi
    done

    echo "Query $query_id never blocked on the lightweight update lock" >&2
    exit 2
}

# Server-side duration, lock try count, lost-CAS retry count and time spent acquiring the lock, for
# the query tagged with $1. The last one is the acquisition window alone, so unlike the duration it
# is not inflated by the update's own work.
function query_stats()
{
    $CLICKHOUSE_CLIENT --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT
            query_duration_ms,
            ProfileEvents['PatchesAcquireLockTries'],
            ProfileEvents['PatchesAcquireLockBadVersionRetries'],
            intDiv(toInt64(ProfileEvents['PatchesAcquireLockMicroseconds']), 1000)
        FROM system.query_log
        WHERE current_database = currentDatabase() AND log_comment = '$1' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1;
    "
}

function run_timeout()
{
    mode=$1
    table_name="t_lwu_timeout_$mode"

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, s String, v UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name VALUES (1, 'aa', 0) (2, 'bb', 0) (3, 'cc', 0);
    "

    # A timeout that expires while the lock is still held must fail with TIMEOUT_EXCEEDED, and must
    # have waited close to that timeout instead of returning at once. The holder is released only
    # after this arm finishes, so the timeout is always the shorter of the two.
    timeout_ms=1000
    start_parked_holder "$table_name" "$mode"

    tag="$run_id-$mode-$timeout_ms"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 200 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = ${timeout_ms}e-3, log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    read -r duration_ms tries _ _ <<< "$(query_stats "$tag")"

    timed_out=0
    if [[ "$error" == *TIMEOUT_EXCEEDED* ]]; then timed_out=1; fi
    # The timeout is what ended the wait: it lasted about that long and no more, and it is not a
    # number of attempts each of which may itself wait the whole timeout. The upper bound is loose
    # enough for a sanitizer runner but far below the multiples an unbounded retry loop produces.
    echo "$mode $timeout_ms failed $timed_out waited $(( duration_ms >= timeout_ms * 9 / 10 && duration_ms < timeout_ms * 10 && tries <= 5 ))"

    release_holder
    wait

    # Cancellation is polled between wait chunks, so a waiter whose max_execution_time is shorter
    # than both the hold and lock_acquire_timeout must die of its own time limit rather than of the
    # lock timeout. A timeout shorter than one wait chunk leaves the whole wait inside a single chunk,
    # so the cancellation also has to be seen once the final chunk returns rather than only before the
    # next one. Which error ends the wait is a fact about where the query got to, so this does not
    # read a clock; an uninterruptible wait reports the lock timeout instead, which is a different
    # error than the query's own limit even though both are TIMEOUT_EXCEEDED, hence matching on the
    # message.
    start_parked_holder "$table_name" "$mode"

    tag="$run_id-$mode-shortcancel"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 600 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 2.5,
                 max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    cancelled=0
    if [[ "$error" == *"Timeout exceeded:"*"maximum:"* ]]; then cancelled=1; fi
    echo "$mode single-chunk cancelled-in-wait $cancelled"

    release_holder
    wait

    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

# A non-replicated table holds the same lock in process memory rather than in Keeper, and both of its
# modes must be as interruptible as the Keeper ones: cancellation is polled between wait chunks in
# one shared helper. Same single-chunk oracle as the arm above, and equally clock-free.
function run_plain()
{
    local mode=$1
    table_name="t_lwu_plain_$mode"

    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, s String, v UInt64)
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name VALUES (1, 'aa', 0) (2, 'bb', 0) (3, 'cc', 0);
    "

    start_parked_holder "$table_name" "$mode" 0

    tag="$run_id-plain-$mode-shortcancel"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 600 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 2.5,
                 max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    cancelled=0
    if [[ "$error" == *"Timeout exceeded:"*"maximum:"* ]]; then cancelled=1; fi
    echo "plain $mode single-chunk cancelled-in-wait $cancelled"

    release_holder
    wait

    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

# The wait is split into chunks that poll query cancellation in between. The holder parks at the
# failpoint, so how long the lock is held is chosen here rather than being the duration of an update,
# which randomized settings are free to change.
function run_watch()
{
    table_name="t_lwu_timeout_auto"
    # The three chunks the bound below requires. The waiter is confirmed to be inside the wait before
    # this hold starts, so it spans at least that many.
    local hold_chunks=3

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, s String, v UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name VALUES (1, 'aa', 0) (2, 'bb', 0) (3, 'cc', 0);
    "

    start_parked_holder "$table_name" "auto"

    # Cancellation between wait chunks: max_execution_time expires inside the first chunk, so only
    # the check at the top of the next iteration interrupts the wait. The lock timeout is finite and
    # far longer than a chunk, so without that check the wait would run until the timeout, and the
    # check after the last chunk would still report the query's own limit. Hence the duration bound.
    tag="$run_id-cancel"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 99 WHERE s LIKE 'xx%'
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 20,
                 max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    read -r duration_ms _ <<< "$(query_stats "$tag")"

    cancelled=0
    if [[ "$error" == *"Timeout exceeded:"*"maximum:"* ]]; then cancelled=1; fi
    # Interrupted within about one chunk rather than waiting out the lock timeout.
    echo "cancel between-chunks $cancelled promptly $(( duration_ms < 10000 ))"

    # A waiter with no max_execution_time parks across several chunks. Chunking the wait must not
    # re-register the watch per chunk: a timed out tryWait deregisters nothing, so that would leave a
    # live callback per chunk on one node. The watch is set by exactly one call, on the conflicting
    # update's node, so a query that waits through N chunks must still register once per outer
    # iteration. The holder is released only after the waiter has been blocked for as many chunks as
    # the bound below requires, so how many chunks it spans is not raced against the holder.
    tag="$run_id-watch"
    $CLICKHOUSE_CLIENT --query_id "$tag" --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 77 WHERE s LIKE 'xx%'
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 600, log_comment = '$tag';
    " &
    waiter_pid=$!

    wait_for_blocked_on_lock "$tag"
    sleep "$(( hold_chunks * 3 ))"
    release_holder
    wait "$waiter_pid"

    # Guard against a vacuous pass: a waiter that spans fewer chunks than the bound below allows
    # would satisfy it even while re-registering per chunk. Three chunks is what makes the bound
    # discriminating, and PatchesAcquireLockMicroseconds is the window that encloses the wait.
    $CLICKHOUSE_CLIENT --query "
        SYSTEM FLUSH LOGS query_log, zookeeper_log;
        WITH
            (
                SELECT (query_id, toInt64(ProfileEvents['PatchesAcquireLockMicroseconds']))
                FROM system.query_log
                WHERE current_database = currentDatabase() AND log_comment = '$tag' AND type = 'QueryFinish'
                ORDER BY event_time_microseconds DESC LIMIT 1
            ) AS waiter,
            (
                SELECT count() FROM system.zookeeper_log
                WHERE type = 'Request' AND has_watch AND query_id = waiter.1
                  AND path LIKE '%/lightweight_updates/in_progress/%'
            ) AS watches
        SELECT 'watch spanned ' || if(waiter.2 >= 3 * 3000 * 1000, 'true', 'false')
            || ' registered_once ' || if(watches BETWEEN 1 AND 2, 'true', 'false');
    "

    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

# Losing the parent-version CAS means some unrelated update committed, so there is no node to watch
# and the retry backs off a fixed amount instead of spinning on Keeper. The victim is parked between
# reading that version and using it, and a second update commits while it is parked, so the victim
# loses the compare-and-swap because the test put a commit in that window rather than because two
# concurrent writers happened to overlap in it.
function run_cas_contention()
{
    table_name="t_lwu_cas"
    tag="$run_id-cas"

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, a UInt64, b UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name SELECT number, 0, 0 FROM numbers(5);
    "

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT patch_parts_lock_pause_before_cas"

    $CLICKHOUSE_CLIENT --query_id "$tag" --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET a = a + 1 WHERE id = 1
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 60, log_comment = '$tag';
    " &
    local victim_pid=$!

    # The wait itself is untimed, so it is bounded here: if nothing ever parks, this reports which
    # step failed instead of hanging until the whole test is killed.
    if ! timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT patch_parts_lock_pause_before_cas PAUSE"
    then
        echo "Failed to park an update before the lightweight update lock compare-and-swap" >&2
        exit 2
    fi

    # `b` is neither read nor written by the victim, so this conflicts with nothing and only bumps
    # the version of the directory the victim is about to write. The failpoint is one-shot, so this
    # update runs straight through.
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET b = b + 1 WHERE id = 1 SETTINGS update_parallel_mode = 'auto';
    "

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT patch_parts_lock_pause_before_cas"
    wait "$victim_pid"

    # Exactly one commit landed in the window, so the victim loses the compare-and-swap once and
    # succeeds on its next attempt. Both counts are chosen by the construction rather than by how
    # fast the runner is. PatchesAcquireLockMicroseconds encloses the parked time, so it cannot say
    # anything about the backoff here and is not asserted.
    read -r _ tries retries _ <<< "$(query_stats "$tag")"
    echo "cas retried_once $(( retries == 1 && tries == 2 ))"

    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

run_timeout "sync"
run_timeout "auto"
run_plain "sync"
run_plain "auto"
run_watch
run_cas_contention
