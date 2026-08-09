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
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_timeout_sync SYNC; DROP TABLE IF EXISTS t_lwu_timeout_auto SYNC" 2>/dev/null || true
}
trap cleanup EXIT

# The failpoint holds the lightweight update lock for 3000 ms, so a conflicting update must wait
# for it. In 'auto' mode a conflict requires one update to READ the column the other WRITES
# (UpdateAffectedColumns::hasConflict), hence the first update writes `s` and the second reads it.
# Two properties are asserted per mode:
#   1. a 1000 ms timeout (shorter than the 3000 ms block) fails with TIMEOUT_EXCEEDED,
#      and the query really waited about that long instead of returning at once;
#   2. a 60000 ms timeout (longer than the block) succeeds.
function run()
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

    for timeout_ms in 1000 60000
    do
        $CLICKHOUSE_CLIENT --query "
            SET enable_lightweight_update = 1;
            SYSTEM ENABLE FAILPOINT $failpoint_name;
            UPDATE $table_name SET s = 'xx' WHERE id = 2 SETTINGS update_parallel_mode = '$mode';
        " &

        wait_for_block_allocated "/zookeeper/$CLICKHOUSE_DATABASE/$table_name/block_numbers/all" "block-0000000001"

        start=$SECONDS
        error=$($CLICKHOUSE_CLIENT --query "
            SET enable_lightweight_update = 1;
            UPDATE $table_name SET v = 200 WHERE s = 'xx'
            SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = ${timeout_ms}e-3;
        " 2>&1 >/dev/null) && error=""
        elapsed=$(( SECONDS - start ))

        if [[ -n "$error" ]]
        then
            # Failed: must be the lock timeout, and must have waited instead of failing at once.
            timed_out=0
            if [[ "$error" == *TIMEOUT_EXCEEDED* ]]; then timed_out=1; fi
            echo "$mode $timeout_ms failed $timed_out waited $(( elapsed >= 1 ))"
        else
            echo "$mode $timeout_ms succeeded"
        fi

        wait
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint_name"
    done

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

run "sync"
run "auto"
