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

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;
    SET enable_lightweight_update = 1;

    DROP TABLE IF EXISTS t_lwu_on_fly SYNC;

    CREATE TABLE t_lwu_on_fly (id UInt64, s String)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_on_fly/', '1') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO t_lwu_on_fly SELECT number, number FROM numbers(1000);
    SYSTEM ENABLE FAILPOINT $failpoint_name;
"

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_on_fly SET s = 'foo' WHERE id >= 500
" &

wait_for_block_allocated "/zookeeper/$CLICKHOUSE_DATABASE/t_lwu_on_fly/block_numbers/all" "block-0000000001"

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_lwu_on_fly UPDATE s = 'bar' WHERE s = 'foo' SETTINGS mutations_sync = 2"

wait

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_lwu_on_fly WHERE s = 'foo' SETTINGS apply_patch_parts = 1;
    SELECT count() FROM t_lwu_on_fly WHERE s = 'bar' SETTINGS apply_patch_parts = 1;
    DROP TABLE t_lwu_on_fly SYNC;
"
