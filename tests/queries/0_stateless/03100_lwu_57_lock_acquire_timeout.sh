#!/usr/bin/env bash
# Tags: long, no-replicated-database, no-parallel
# long: the failpoint holds and the ~20 s cancellation fixture put this at about 56 s.
# no-replicated-database - path in zookeeper differs with replicated database
# no-parallel: the `*_lightweight_update_sleep_after_block_allocation` failpoint fires exactly
#   once globally; a concurrent run of a sibling 03100_lwu_* test could steal the pause or
#   disable the failpoint before this test's UPDATE reaches the injection site.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

set -e

failpoint_name="rmt_lightweight_update_sleep_after_block_allocation"
storage_policy=`$CLICKHOUSE_CLIENT -q "SELECT value FROM system.merge_tree_settings WHERE name = 'storage_policy'"`

if [[ "$storage_policy" == "s3_with_keeper" ]]; then
    failpoint_name="smt_lightweight_update_sleep_after_block_allocation"
fi

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint_name" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_timeout_sync SYNC; DROP TABLE IF EXISTS t_lwu_timeout_auto SYNC; DROP TABLE IF EXISTS t_lwu_cas SYNC; DROP TABLE IF EXISTS t_lwu_cancel SYNC" 2>/dev/null || true
}
trap cleanup EXIT

# The failpoint holds the lightweight update lock for 3000 ms, so a conflicting update must wait
# for it. In 'auto' mode a conflict requires one update to READ the column the other WRITES
# (UpdateAffectedColumns::hasConflict), hence the first update writes `s` and the second reads it.
hold_ms=3000

# Starts the blocking update and returns once it holds the lock.
function start_holder()
{
    local table_name=$1
    local mode=$2

    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        SYSTEM ENABLE FAILPOINT $failpoint_name;
        UPDATE $table_name SET s = 'xx' WHERE id = 2 SETTINGS update_parallel_mode = '$mode';
    " &

    wait_for_block_allocated "/zookeeper/$CLICKHOUSE_DATABASE/$table_name/block_numbers/all" "block-0000000001"
}

# Server-side duration, lock try count and lost-CAS retry count of the query tagged with $1.
function query_stats()
{
    $CLICKHOUSE_CLIENT --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT
            query_duration_ms,
            ProfileEvents['PatchesAcquireLockTries'],
            ProfileEvents['PatchesAcquireLockBadVersionRetries']
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

    # 1. A timeout shorter than the hold must fail with TIMEOUT_EXCEEDED, and must really have
    #    waited about that long instead of returning at once. 2. A longer one must succeed.
    for timeout_ms in 1000 60000
    do
        start_holder "$table_name" "$mode"

        tag="lwu57-$mode-$timeout_ms-$CLICKHOUSE_DATABASE"
        error=$($CLICKHOUSE_CLIENT --query "
            SET enable_lightweight_update = 1;
            UPDATE $table_name SET v = 200 WHERE s = 'xx'
            SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = ${timeout_ms}e-3, log_comment = '$tag';
        " 2>&1 >/dev/null) && error=""

        read -r duration_ms _ <<< "$(query_stats "$tag")"

        if [[ -n "$error" ]]
        then
            # Failed: must be the lock timeout, and must have waited close to its own timeout --
            # neither returning at once nor sitting there for the whole hold.
            timed_out=0
            if [[ "$error" == *TIMEOUT_EXCEEDED* ]]; then timed_out=1; fi
            echo "$mode $timeout_ms failed $timed_out waited $(( duration_ms >= timeout_ms * 9 / 10 && duration_ms < hold_ms ))"
        else
            echo "$mode $timeout_ms succeeded"
        fi

        wait
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint_name"
    done

    # A timeout far larger than the hold must still WAIT for the lock rather than give up at once.
    # Values near INT64_MAX / 1e6 are deliberately not used: the outer table lock converts the same
    # setting to a nanosecond deadline (RWLock.cpp) and overflows first, which is a pre-existing
    # limitation of every lock_acquire_timeout consumer and outside the scope of this change.
    start_holder "$table_name" "$mode"

    tag="lwu57-$mode-huge-$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 400 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 1000000, log_comment = '$tag';
    "
    read -r duration_ms _ <<< "$(query_stats "$tag")"
    echo "$mode huge-timeout succeeded waited $(( duration_ms >= 500 ))"

    wait
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint_name"

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

    start_holder "$table_name" "auto"

    # Churn: `w` is neither read nor written by the holder or the waiter, so these conflict with
    # nothing and only change the `in_progress` directory.
    (
        end=$((SECONDS + 8))
        while [[ $SECONDS -lt $end ]]
        do
            $CLICKHOUSE_CLIENT --query "
                SET enable_lightweight_update = 1;
                UPDATE $table_name SET w = w + 1 WHERE id = 1 SETTINGS update_parallel_mode = 'auto';
            " 2>/dev/null || true
        done
    ) &
    churn_pid=$!

    tag="lwu57-churn-$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 500 WHERE s = 'xx'
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 60, log_comment = '$tag';
    "

    kill $churn_pid 2>/dev/null || true
    wait $churn_pid 2>/dev/null || true

    read -r _ tries _ <<< "$(query_stats "$tag")"
    # One try to find the conflict, one to acquire after it clears. A watch on the parent directory
    # instead wakes once per churn commit and needs an order of magnitude more.
    echo "churn succeeded tries_bounded $(( tries >= 1 && tries <= 5 ))"

    wait
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint_name"
    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

# The wait is split into chunks that poll query cancellation in between, so max_execution_time and
# KILL QUERY take effect instead of being deferred until the whole lock_acquire_timeout has elapsed.
# Needs a hold longer than one chunk, which the 3000 ms failpoint cannot give: 8 unmerged parts of
# 10 rows sleeping 0.25 s per row is 2.5 s per block (under the 3 s per block sleep limit) and about
# 20 s in total, and the lock is held for the whole update.
function run_cancel()
{
    table_name="t_lwu_cancel"

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, s String, v UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        SYSTEM STOP MERGES $table_name;
    "

    for part in {0..7}
    do
        $CLICKHOUSE_CLIENT --query "
            SET insert_keeper_fault_injection_probability = 0.0;
            INSERT INTO $table_name SELECT number + $part * 10, 'bb', 0 FROM numbers(10);
        "
    done

    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET s = 'xx' || toString(sleepEachRow(0.25)) WHERE id >= 0
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 600, max_threads = 1;
    " &

    # Wait until the slow update owns the lock.
    for _ in {0..100}
    do
        sleep 0.3
        held=$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.zookeeper
            WHERE path = '/zookeeper/$CLICKHOUSE_DATABASE/$table_name/lightweight_updates/in_progress';
        ")
        if [[ "$held" -gt 0 ]]; then break; fi
    done

    # max_execution_time rather than KILL QUERY: both are enforced by QueryStatus::checkTimeLimit(),
    # and this needs no second client racing to catch the waiter in system.processes.
    tag="lwu57-cancel-$CLICKHOUSE_DATABASE"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 99 WHERE s LIKE 'xx%'
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 600,
                 max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    read -r duration_ms _ <<< "$(query_stats "$tag")"

    cancelled=0
    if [[ "$error" == *"maximum: 2000 ms"* ]]; then cancelled=1; fi
    # Interrupted within about one chunk, not held to the end of the ~20 s update.
    echo "cancel interrupted $cancelled promptly $(( duration_ms < 8000 ))"

    # The lock is still held for most of that ~20 s update, so one more waiter with no
    # max_execution_time parks here across several chunks. Chunking the wait must not re-register the
    # watch per chunk: a timed out tryWait deregisters nothing, so that would leave a live callback
    # per chunk on one node. The watch is set by exactly one call, on the conflicting update's node,
    # so a query that waits through N chunks must still register once per outer iteration.
    tag="lwu57-watch-$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 77 WHERE s LIKE 'xx%'
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 600, log_comment = '$tag';
    "

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
# and the retry backs off a fixed amount instead of spinning on Keeper. The writers below touch
# pairwise-disjoint columns, so none of them ever finds a column conflict: every one goes straight to
# the CAS, and whoever overlaps another's read-then-CAS window loses it.
function run_cas_contention()
{
    table_name="t_lwu_cas"
    local writers=6

    local cols="" vals=""
    for k in $(seq 0 $((writers - 1)))
    do
        cols="$cols, c$k UInt64"
        vals="$vals, 0"
    done

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64 $cols)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name SELECT number $vals FROM numbers(5);
    "

    tag="lwu57-cas-$CLICKHOUSE_DATABASE"
    for k in $(seq 0 $((writers - 1)))
    do
        (
            end=$((SECONDS + 4))
            while [[ $SECONDS -lt $end ]]
            do
                $CLICKHOUSE_CLIENT --query "
                    SET enable_lightweight_update = 1;
                    UPDATE $table_name SET c$k = c$k + 1 WHERE id = 1
                    SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 60, log_comment = '$tag';
                " 2>/dev/null || true
            done
        ) &
    done
    wait

    # Every retry sleeps 50 ms inside the lock acquisition, which is what
    # PatchesAcquireLockMicroseconds measures, so acquiring must have taken at least that long. An
    # inequality implied by the sleep itself, not a tuned threshold. The first condition also guards
    # against a vacuous pass: without any retry the second one holds trivially.
    $CLICKHOUSE_CLIENT --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT
            'cas reached ' || if(countIf(retries > 0) > 0, 'true', 'false')
                || ' backed_off ' || if(minIf(acquire_us - retries * 50000, retries > 0) >= 0, 'true', 'false')
        FROM
        (
            SELECT
                toInt64(ProfileEvents['PatchesAcquireLockBadVersionRetries']) AS retries,
                toInt64(ProfileEvents['PatchesAcquireLockMicroseconds']) AS acquire_us
            FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '$tag' AND type = 'QueryFinish'
        );
    "

    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

run "sync"
run "auto"
run_churn
run_cas_contention
run_cancel
