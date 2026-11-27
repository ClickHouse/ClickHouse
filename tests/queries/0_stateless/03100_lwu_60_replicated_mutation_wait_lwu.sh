#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel, no-fasttest, no-shared-merge-tree
# no-replicated-database: the path in zookeeper differs with a replicated database
# no-parallel: the `rmt_lightweight_update_sleep_after_block_allocation` failpoint fires once globally
# no-shared-merge-tree: the test stops pulling the replication log of one replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

failpoint_name="rmt_lightweight_update_sleep_after_block_allocation"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $failpoint_name" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM START PULLING REPLICATION LOG t_lwu_60_r2" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES t_lwu_60_r1" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_60_r1 SYNC" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_60_r2 SYNC" 2>/dev/null || true
}
trap cleanup EXIT

function count_mutate_entries_r2()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 't_lwu_60_r2' AND type = 'MUTATE_PART'"
}

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_60_r1 SYNC;
    DROP TABLE IF EXISTS t_lwu_60_r2 SYNC;

    CREATE TABLE t_lwu_60_r1 (id UInt64, s String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_lwu_60', '1') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    CREATE TABLE t_lwu_60_r2 (id UInt64, s String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_lwu_60', '2') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO t_lwu_60_r1 SELECT number, toString(number) FROM numbers(1000) SETTINGS insert_keeper_fault_injection_probability = 0;
    SYSTEM SYNC REPLICA t_lwu_60_r2;

    -- The replica that has the patch must not execute the mutation first.
    SYSTEM STOP MERGES t_lwu_60_r1;
    SYSTEM ENABLE FAILPOINT $failpoint_name;
"

$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --insert_keeper_fault_injection_probability 0 \
    --query "UPDATE t_lwu_60_r1 SET s = 'foo' WHERE id >= 500" &
update_pid=$!

for _ in {1..300}; do
    res=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_lwu_60/block_numbers/all' AND name = 'block-0000000001'")
    [[ "$res" -gt 0 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_lwu_60_r1 DELETE WHERE id < 10 SETTINGS mutations_sync = 0"

# While the update is not committed, make the second replica stop pulling the log as soon as it has the mutation entry.
while kill -0 "$update_pid" 2>/dev/null; do
    [[ $(count_mutate_entries_r2) -gt 0 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "SYSTEM STOP PULLING REPLICATION LOG t_lwu_60_r2"
wait "$update_pid"

for _ in {1..300}; do
    [[ $(count_mutate_entries_r2) -eq 0 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM START PULLING REPLICATION LOG t_lwu_60_r2;
    SYSTEM START MERGES t_lwu_60_r1;
"

for _ in {1..1200}; do
    res=$($CLICKHOUSE_CLIENT --query "SELECT countIf(is_done) FROM system.mutations WHERE database = currentDatabase() AND table IN ('t_lwu_60_r1', 't_lwu_60_r2')")
    [[ "$res" -eq 2 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM SYNC REPLICA t_lwu_60_r1;
    SYSTEM SYNC REPLICA t_lwu_60_r2;

    SELECT 'r1', countIf(s = 'foo'), count() FROM t_lwu_60_r1;
    SELECT 'r2', countIf(s = 'foo'), count() FROM t_lwu_60_r2;
"
