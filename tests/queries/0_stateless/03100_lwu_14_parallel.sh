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

# Disable the process-global ONCE failpoint, reap the background client and drop the
# replicated tables on any exit path, so a wait_for_block_allocated timeout can't leave the
# failpoint armed or an orphaned zookeeper path behind for a later test.
function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint_name" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_parallel_sync SYNC; DROP TABLE IF EXISTS t_lwu_parallel_auto SYNC" 2>/dev/null || true
}
trap cleanup EXIT

function run()
{
    mode=$1
    table_name="t_lwu_parallel_$mode"

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

    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        SYSTEM ENABLE FAILPOINT $failpoint_name;
        UPDATE $table_name SET s = 'xx' WHERE id = 2 SETTINGS update_parallel_mode = '$mode';
    " &

    wait_for_block_allocated "/zookeeper/$CLICKHOUSE_DATABASE/$table_name/block_numbers/all" "block-0000000001"

    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 200 WHERE s = 'xx' SETTINGS update_parallel_mode = '$mode';
    " &

    wait;

    $CLICKHOUSE_CLIENT --query "
        SELECT * FROM $table_name ORDER BY id;
        DROP TABLE $table_name SYNC;
    "
}

run "sync"
run "auto"
