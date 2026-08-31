#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: uses a global failpoint and two replicas with stopped replication queues.
# no-replicated-database: the test controls replication queues of individual tables.
# no-shared-merge-tree: the failpoint is in the `ReplicatedMergeTree` block allocation path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./parts.lib
. "$CUR_DIR"/parts.lib

set -e

R1=t_lwu_prune_empty_03100_58_r1
R2=t_lwu_prune_empty_03100_58_r2

function cleanup()
{
	$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_lightweight_update_sleep_after_block_allocation" 2>/dev/null || true
	$CLICKHOUSE_CLIENT --query "SYSTEM START REPLICATION QUEUES $R1" 2>/dev/null || true
	$CLICKHOUSE_CLIENT --query "SYSTEM START REPLICATION QUEUES $R2" 2>/dev/null || true
	wait || true
	$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $R2 SYNC" 2>/dev/null || true
	$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $R1 SYNC" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
	SET insert_keeper_fault_injection_probability = 0.0;

	DROP TABLE IF EXISTS $R2 SYNC;
	DROP TABLE IF EXISTS $R1 SYNC;

	CREATE TABLE $R1 (p UInt8, x UInt64, v UInt64)
	ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_lwu_prune_empty_03100_58', 'r1')
	PARTITION BY p ORDER BY x
	SETTINGS
		enable_block_number_column = 1,
		enable_block_offset_column = 1,
		remove_empty_parts = 0;

	CREATE TABLE $R2 (p UInt8, x UInt64, v UInt64)
	ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_lwu_prune_empty_03100_58', 'r2')
	PARTITION BY p ORDER BY x
	SETTINGS
		enable_block_number_column = 1,
		enable_block_offset_column = 1,
		remove_empty_parts = 0;

	INSERT INTO $R1 SELECT 1, number, 1 FROM numbers(50);
	INSERT INTO $R1 SELECT 2, number, 1 FROM numbers(50);
	SYSTEM SYNC REPLICA $R2;

	ALTER TABLE $R1 DELETE WHERE p = 2 SETTINGS mutations_sync = 2;

	SYSTEM STOP REPLICATION QUEUES $R1;
"

$CLICKHOUSE_CLIENT --query "INSERT INTO $R2 SELECT 2, 1000000 + number, 1 FROM numbers(30)"

next_block_number=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_lwu_prune_empty_03100_58/block_numbers/2'")
next_block_name=$(printf 'block-%010d' "$next_block_number")

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT rmt_lightweight_update_sleep_after_block_allocation"

$CLICKHOUSE_CLIENT --query "UPDATE $R1 SET v = 999 WHERE p = 2 SETTINGS enable_lightweight_update = 1" &

wait_for_block_allocated "/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_lwu_prune_empty_03100_58/block_numbers/2" "$next_block_name"

$CLICKHOUSE_CLIENT --query "SYSTEM START REPLICATION QUEUES $R1"

wait

$CLICKHOUSE_CLIENT --query "
	SYSTEM SYNC REPLICA $R1;
	SYSTEM SYNC REPLICA $R2;

	SELECT count() FROM $R1 WHERE p = 2 AND v = 1 SETTINGS apply_patch_parts = 1;
	SELECT count() FROM $R2 WHERE p = 2 AND v = 1 SETTINGS apply_patch_parts = 1;
"
