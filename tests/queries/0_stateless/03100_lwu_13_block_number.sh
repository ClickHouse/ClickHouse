#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: failpoint is enabled only on one replica.

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

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;
    DROP TABLE IF EXISTS t_lwu_block_number SYNC;

    CREATE TABLE t_lwu_block_number (id UInt64, s String)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_block_number/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1;

    INSERT INTO t_lwu_block_number SELECT number, number FROM numbers(1000);
    SYSTEM ENABLE FAILPOINT $failpoint_name;
"

$CLICKHOUSE_CLIENT --query "UPDATE t_lwu_block_number SET s = 'foo' WHERE id >= 500" --enable_lightweight_update 1 &

wait_for_block_allocated "/zookeeper/$CLICKHOUSE_DATABASE/t_lwu_block_number/block_numbers/all" "block-0000000001"

$CLICKHOUSE_CLIENT --query "INSERT INTO t_lwu_block_number SELECT number, number FROM numbers(1000, 1000)"

wait

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_lwu_block_number WHERE s = 'foo' SETTINGS apply_patch_parts = 1;
    SELECT sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_block_number' AND startsWith(name, 'patch');
    DROP TABLE t_lwu_block_number SYNC;
"
