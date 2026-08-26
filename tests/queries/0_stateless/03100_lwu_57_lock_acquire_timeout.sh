#!/usr/bin/env bash
# Tags: long, no-replicated-database, no-parallel
# long: the lock holds put this at about a minute.
# no-replicated-database - path in zookeeper differs with replicated database
# no-parallel: the `infinite_sleep` and `patch_parts_lock_pause_before_cas` failpoints are
#   server-global, so a concurrent test would park at them or clear them while this one waits.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# The log tables outlive the tables this test drops, and `clickhouse-test --database` reuses one
# database for every test, so tagging by database alone would let one run match another's rows.
run_id="lwu57-$CLICKHOUSE_DATABASE-$RANDOM$RANDOM"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT infinite_sleep" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT patch_parts_lock_pause_before_cas" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_timeout_sync SYNC; DROP TABLE IF EXISTS t_lwu_timeout_auto SYNC; DROP TABLE IF EXISTS t_lwu_cas SYNC; DROP TABLE IF EXISTS t_lwu_cancel SYNC; DROP TABLE IF EXISTS t_lwu_plain_sync SYNC; DROP TABLE IF EXISTS t_lwu_plain_auto SYNC" 2>/dev/null || true
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

# Starts an update that takes the lock and then parks inside `sleep` at the `infinite_sleep`
# failpoint, holding the lock until release_holder is called. The hold does not begin expiring before
# the waiter starts, so how long a waiter blocks is chosen by this test rather than raced against a
# fixed sleep.
function start_parked_holder()
{
    local table_name=$1
    local mode=$2
    # A plain MergeTree keeps the lock in process memory, so there is no node to count.
    local in_keeper=${3:-1}

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT infinite_sleep"

    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET s = 'xx' WHERE id = 2 AND sleep(0.001) = 0
        SETTINGS update_parallel_mode = '$mode';
    " &

    if ! $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT infinite_sleep PAUSE"
    then
        echo "Failed to park a $mode holder of the lightweight update lock on $table_name" >&2
        exit 2
    fi

    # The pause is reached after the lock is taken, so this must already hold.
    if [[ "$in_keeper" == "1" ]]
    then
        wait_for_lock_held "$table_name" "$mode"
    fi
}

# Lets the parked holder finish and release the lock. Callers wait for the background jobs they
# started themselves, so that a waiter can be waited for separately from the holder.
function release_holder()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT infinite_sleep"
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

function run()
{
    mode=$1
    table_name="t_lwu_timeout_$mode"

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, s String, v UInt64, w UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name VALUES (1, 'aa', 0, 0) (2, 'bb', 0, 0) (3, 'cc', 0, 0);
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

    # A timeout longer than the hold must wait for the lock and then be granted it, for a plain and
    # for a very large value. The waiter is confirmed to be inside the wait before the release, so a
    # grant that took more than one attempt orders the two events without timing either.
    for arm in "60000 60" "huge-timeout 1000000"
    do
        read -r label timeout_s <<< "$arm"
        start_parked_holder "$table_name" "$mode"

        tag="$run_id-$mode-$label"
        $CLICKHOUSE_CLIENT --query_id "$tag" --query "
            SET enable_lightweight_update = 1;
            UPDATE $table_name SET v = 400 WHERE s = 'xx'
            SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = $timeout_s, log_comment = '$tag';
        " &
        waiter_pid=$!

        wait_for_blocked_on_lock "$tag"
        release_holder
        wait "$waiter_pid"

        read -r _ tries _ _ <<< "$(query_stats "$tag")"
        echo "$mode $label succeeded contended $(( tries >= 2 ))"

        wait
    done

    # Cancellation is polled between wait chunks, so a waiter whose max_execution_time is shorter
    # than both the hold and lock_acquire_timeout must die of its own time limit rather than of the
    # lock timeout. Which error ends the wait is a fact about where the query got to, so this does
    # not read a clock; an uninterruptible wait reports the lock timeout instead.
    start_parked_holder "$table_name" "$mode"

    tag="$run_id-$mode-cancel"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 500 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 30,
                 max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    cancelled=0
    if [[ "$error" == *"Timeout exceeded:"*"maximum:"* ]]; then cancelled=1; fi
    echo "$mode cancelled-in-wait $cancelled"

    release_holder
    wait

    # A timeout shorter than one wait chunk leaves the whole wait inside a single chunk, so the
    # cancellation has to be seen once the chunk returns rather than only before the next one.
    # Otherwise this reports the lock timing out, which is a different error than the query's own
    # limit even though both are TIMEOUT_EXCEEDED, hence matching on the message.
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

    # An uncontended update must still be granted with lock_acquire_timeout = 0, which is the
    # current behaviour of both Keeper modes and is intentionally left unchanged.
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 300 WHERE id = 1
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 0;
    "
    echo "$mode zero-timeout uncontended succeeded"

    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

# A non-replicated table holds the same lock in process memory rather than in Keeper, and both of its
# modes must be as interruptible as the Keeper ones: cancellation is polled between wait chunks in
# one shared helper. Same oracle as the arm above, and equally clock-free.
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

    tag="$run_id-plain-$mode-cancel"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 500 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 30,
                 max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    cancelled=0
    if [[ "$error" == *"Timeout exceeded:"*"maximum:"* ]]; then cancelled=1; fi
    echo "plain $mode cancelled-in-wait $cancelled"

    release_holder
    wait

    # Same single-chunk case as in Keeper: one shared helper serves all four paths.
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

    # The unconditional first attempt is what keeps a zero timeout working, here as in Keeper.
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 300 WHERE id = 1
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 0;
    "
    echo "plain $mode zero-timeout uncontended succeeded"

    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

# Waiting on the conflicting update's own node rather than on the `in_progress` parent directory:
# unrelated updates keep changing the parent, so a watch there wakes the waiter over and over
# without it being able to progress. Only 'auto' mode reads that directory.
function run_churn()
{
    table_name="t_lwu_timeout_auto"

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, s String, v UInt64, w UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name VALUES (1, 'aa', 0, 0) (2, 'bb', 0, 0) (3, 'cc', 0, 0);
    "

    start_parked_holder "$table_name" "auto"

    # Churn: `w` is neither read nor written by the holder or the waiter, so these conflict with
    # nothing and only change the `in_progress` directory.
    # Bounded as well as stoppable, so that killing the test cannot leave the loop behind updating a
    # dropped table.
    churn_stop="$CLICKHOUSE_TMP/${run_id}_churn_stop"
    rm -f "$churn_stop"
    (
        end=$((SECONDS + 120))
        while [[ ! -e "$churn_stop" && $SECONDS -lt $end ]]
        do
            $CLICKHOUSE_CLIENT --query "
                SET enable_lightweight_update = 1;
                UPDATE $table_name SET w = w + 1 WHERE id = 1 SETTINGS update_parallel_mode = 'auto';
            " 2>/dev/null || true
        done
    ) &
    churn_pid=$!

    tag="$run_id-churn"
    $CLICKHOUSE_CLIENT --query_id "$tag" --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 500 WHERE s = 'xx'
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 60, log_comment = '$tag';
    " &
    waiter_pid=$!

    wait_for_blocked_on_lock "$tag"

    # A watch on the parent directory wakes once per commit anywhere under it, so the churn has to
    # commit repeatedly while the waiter is inside the wait for the try count below to tell the two
    # watch targets apart. `w` counts the churn's own commits, which no runner speed can change.
    churned=0
    for _ in {0..600}
    do
        sleep 0.1
        if [[ "$($CLICKHOUSE_CLIENT --query "SELECT w FROM $table_name WHERE id = 1")" -ge 10 ]]
        then
            churned=1
            break
        fi
    done

    if [[ "$churned" != 1 ]]
    then
        echo "Churn never committed enough times to wake a parent directory watch" >&2
        exit 2
    fi

    # Churn stops before the lock is released. Those commits have already woken a parent directory
    # watch as many times as they are going to, and once the waiter is runnable they would instead
    # make it lose the parent version compare-and-swap an unbounded number of times.
    touch "$churn_stop"
    wait $churn_pid 2>/dev/null || true
    rm -f "$churn_stop"

    release_holder
    wait "$waiter_pid"

    read -r _ tries _ <<< "$(query_stats "$tag")"
    # One try to find the conflict, one to acquire after it clears. A watch on the parent directory
    # instead wakes once per churn commit and needs an order of magnitude more.
    echo "churn succeeded tries_bounded $(( tries >= 2 && tries <= 5 ))"

    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

# The wait is split into chunks that poll query cancellation in between, so max_execution_time and
# KILL QUERY take effect instead of being deferred until the whole lock_acquire_timeout has elapsed.
# The holder parks at the failpoint, so how long the lock is held is chosen here rather than being
# the duration of an update, which randomized settings are free to change.
function run_cancel()
{
    table_name="t_lwu_cancel"
    # More than the three chunks the watch arm below requires, so that arm cannot pass vacuously.
    local hold_chunks=4

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

    # max_execution_time rather than KILL QUERY: both are enforced by QueryStatus::checkTimeLimit(),
    # and this needs no second client racing to catch the waiter in system.processes.
    tag="$run_id-cancel"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 99 WHERE s LIKE 'xx%'
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 600,
                 max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    read -r duration_ms _ <<< "$(query_stats "$tag")"

    cancelled=0
    if [[ "$error" == *"Timeout exceeded:"*"maximum:"* ]]; then cancelled=1; fi
    # Interrupted within about one chunk rather than waiting out the hold.
    echo "cancel interrupted $cancelled promptly $(( duration_ms < 8000 ))"

    # A waiter with no max_execution_time parks across several chunks. Chunking the wait must not
    # re-register the watch per chunk: a timed out tryWait deregisters nothing, so that would leave a
    # live callback per chunk on one node. The watch is set by exactly one call, on the conflicting
    # update's node, so a query that waits through N chunks must still register once per outer
    # iteration. The holder is released only after the waiter has been blocked for more chunks than
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

run "sync"
run "auto"
run_plain "sync"
run_plain "auto"
run_churn
run_cas_contention
run_cancel
