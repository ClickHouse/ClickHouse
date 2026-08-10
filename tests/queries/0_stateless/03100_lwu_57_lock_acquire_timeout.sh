#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel
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
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_timeout_sync SYNC; DROP TABLE IF EXISTS t_lwu_timeout_auto SYNC; DROP TABLE IF EXISTS t_lwu_cancel SYNC" 2>/dev/null || true
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

# Server-side duration and lock try count of the query tagged with $1.
function query_stats()
{
    $CLICKHOUSE_CLIENT --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT query_duration_ms, ProfileEvents['PatchesAcquireLockTries']
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

    # A huge but valid timeout must still WAIT for the lock. Deriving a deadline by adding the
    # timeout to a steady_clock reading overflows here and yields an already-expired deadline,
    # which would fail on the first try instead.
    start_holder "$table_name" "$mode"

    tag="lwu57-$mode-huge-$CLICKHOUSE_DATABASE"
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 400 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 10000000000, log_comment = '$tag';
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

    read -r _ tries <<< "$(query_stats "$tag")"
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

    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

run "sync"
run "auto"
run_churn
run_cancel
