#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel
# no-replicated-database: failpoint is enabled only on one replica.
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
# replicated table on any exit path, so a wait_for_block_allocated timeout can't leave the
# failpoint armed or an orphaned zookeeper path behind for a later test.
function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint_name" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_future_reads SYNC" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0.0;
    DROP TABLE IF EXISTS t_lwu_future_reads SYNC;

    CREATE TABLE t_lwu_future_reads (id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_future_reads/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        -- The test checks the number of rows in patch parts at the end.
        -- Once the patches are applied, the cleanup thread is free to drop them,
        -- so keep them around to make the last query deterministic.
        remove_unused_patch_parts = 0;

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
