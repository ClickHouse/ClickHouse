#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: failpoint is enabled only on one replica.

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

failpoint_name="rmt_lightweight_update_sleep_after_block_allocation"
storage_policy="SELECT value FROM system.merge_tree_settings WHERE name = 'storage_policy'"

if [[ "$storage_policy" == "s3_with_keeper" ]]; then
    failpoint_name="smt_lightweight_update_sleep_after_block_allocation"
fi

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_block_number SYNC;
    SET allow_experimental_lightweight_update = 1;

    CREATE TABLE t_lwu_block_number (id UInt64, c1 UInt64, c2 String)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_block_number/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1;

    INSERT INTO t_lwu_block_number VALUES (1, 100, 'aa') (2, 200, 'bb') (3, 300, 'cc');
    SYSTEM ENABLE FAILPOINT $failpoint_name;
"

$CLICKHOUSE_CLIENT --query "
    SET allow_experimental_lightweight_update = 1;
    UPDATE t_lwu_block_number SET c1 = c1 * 10 WHERE c2 = 'aa'
" &

wait_for_block_allocated "/zookeeper/$CLICKHOUSE_DATABASE/t_lwu_block_number/block_numbers/all" "block-0000000001"

$CLICKHOUSE_CLIENT --query "
    SET allow_experimental_lightweight_update = 1;
    UPDATE t_lwu_block_number SET c2 = 'xx' WHERE id = 1;
    UPDATE t_lwu_block_number SET c2 = 'aa' WHERE id = 2;
"

wait

$CLICKHOUSE_CLIENT --query "
    SELECT * FROM t_lwu_block_number ORDER BY id;
    DROP TABLE t_lwu_block_number SYNC;
"
