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
    DROP TABLE IF EXISTS t_lwu_future_reads SYNC;

    CREATE TABLE t_lwu_future_reads (id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_future_reads/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1;

    INSERT INTO t_lwu_future_reads SELECT number, number FROM numbers(1000);
    SYSTEM ENABLE FAILPOINT $failpoint_name;
"

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_future_reads SET v = v + 1000 WHERE id >= 100 AND id < 200
" &

wait_for_block_allocated "/zookeeper/$CLICKHOUSE_DATABASE/t_lwu_future_reads/block_numbers/all" "block-0000000001"

$CLICKHOUSE_CLIENT --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_future_reads SET v = v + 2000 WHERE id >= 200 AND id < 300;
    OPTIMIZE TABLE t_lwu_future_reads PARTITION ID 'all' FINAL;
"

wait

$CLICKHOUSE_CLIENT --query "
    SELECT sum(v) FROM t_lwu_future_reads SETTINGS apply_patch_parts = 1;
    SELECT sum(multiIf (number >= 100 AND number < 200, number + 1000, number >= 200 AND number < 300, number + 2000, number)) FROM numbers(1000);

    SELECT sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_future_reads' AND startsWith(name, 'patch');
    DROP TABLE t_lwu_future_reads SYNC;
"
