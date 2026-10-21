#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

set -e

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_block_number SYNC;
    SET enable_lightweight_update = 1;
    SET insert_keeper_fault_injection_probability = 0.0;

    CREATE TABLE t_lwu_block_number (id UInt64,s String)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_block_number/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1;

    INSERT INTO t_lwu_block_number VALUES (1, 'aa') (2, 'bb') (3, 'cc');
    UPDATE t_lwu_block_number SET s = 'foo' WHERE id = 1;
"

failpoint_name="rmt_merge_task_sleep_in_prepare"
storage_policy=`$CLICKHOUSE_CLIENT -q "SELECT value FROM system.merge_tree_settings WHERE name = 'storage_policy'"`

if [[ "$storage_policy" == "s3_with_keeper" ]]; then
    failpoint_name="smt_merge_task_sleep_in_prepare"
fi

$CLICKHOUSE_CLIENT --query "
    SET optimize_throw_if_noop = 1;
    SYSTEM ENABLE FAILPOINT $failpoint_name;
    OPTIMIZE TABLE t_lwu_block_number PARTITION ID 'all' FINAL;
" &

sleep 1.0

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_block_number SET s = 'bar' WHERE id = 2;
    OPTIMIZE TABLE t_lwu_block_number PARTITION ID 'patch-8feeedf7588c601fd7f38da7fe68712b-all' FINAL;
"

wait

$CLICKHOUSE_CLIENT --query "
    SELECT * FROM t_lwu_block_number ORDER BY id SETTINGS apply_patch_parts = 1;

    SET optimize_throw_if_noop = 1;
    OPTIMIZE TABLE t_lwu_block_number PARTITION ID 'all' FINAL;

    SELECT * FROM t_lwu_block_number ORDER BY id SETTINGS apply_patch_parts = 0;
    DROP TABLE t_lwu_block_number SYNC;
"
