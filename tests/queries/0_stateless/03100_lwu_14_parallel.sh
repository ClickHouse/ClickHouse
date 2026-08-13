#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database - path in zookeeper differs with replicated database

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
