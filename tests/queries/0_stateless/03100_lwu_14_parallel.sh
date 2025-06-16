#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

function wait_for_block_allocated()
{
    path="$1"
    block_number="$2"

    for _ in {0..50}; do
        sleep 0.1
        res=`$CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.zookeeper
            WHERE path = '$path' AND name = '$block_number'
        "`
        if [[ "$res" -eq "1" ]]; then
            break
        fi
    done
}

function run()
{
    mode=$1

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS t_lwu_parallel SYNC;

        CREATE TABLE t_lwu_parallel (id UInt64, s String, v UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_parallel/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO t_lwu_parallel VALUES (1, 'aa', 0) (2, 'bb', 0) (3, 'cc', 0);
    "

    $CLICKHOUSE_CLIENT --query "
        SET allow_experimental_lightweight_update = 1;
        SYSTEM ENABLE FAILPOINT smt_lightweight_update_sleep_after_block_allocation;
        UPDATE t_lwu_parallel SET s = 'xx' WHERE id = 2 SETTINGS update_parallel_mode = '$mode';
    " &

    wait_for_block_allocated "/zookeeper/$CLICKHOUSE_DATABASE/t_lwu_parallel/block_numbers/all" "block-0000000001"

    $CLICKHOUSE_CLIENT --query "
        SET allow_experimental_lightweight_update = 1;
        UPDATE t_lwu_parallel SET v = 200 WHERE s = 'xx' SETTINGS update_parallel_mode = '$mode';
    " &

    wait;

    $CLICKHOUSE_CLIENT --query "
        SELECT * FROM t_lwu_parallel ORDER BY id;
        DROP TABLE t_lwu_parallel SYNC;
    "
}

run "sync"
run "auto"
