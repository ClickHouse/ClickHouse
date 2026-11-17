#!/usr/bin/env bash
# Tags: replica, zookeeper


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SYNC_ALL="
SYSTEM SYNC REPLICA data_r1;
SYSTEM SYNC REPLICA data_r2;
SYSTEM SYNC REPLICA data_r3;
"

###############################################################################
# Shared setup: 3 replicas, rf=2
###############################################################################
$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS data_r1 SYNC;
DROP TABLE IF EXISTS data_r2 SYNC;
DROP TABLE IF EXISTS data_r3 SYNC;

CREATE TABLE data_r1 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_merge/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;

CREATE TABLE data_r2 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_merge/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;

CREATE TABLE data_r3 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_merge/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;
"

###############################################################################
# Test 1: OPTIMIZE on assigned replica executes locally
# Insert extra parts into partition 1, then OPTIMIZE on the assigned replica.
###############################################################################
$CLICKHOUSE_CLIENT -n --query "
INSERT INTO data_r1 SELECT number, number * 10, 1 FROM numbers(3);
INSERT INTO data_r1 (key, value, part_key) VALUES (100, 1000, 1);
INSERT INTO data_r1 (key, value, part_key) VALUES (101, 1010, 1);
${SYNC_ALL}
"

echo "before_optimize_parts_in_partition_1"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = 'data_r1'
      AND active
      AND partition_id = '1'
"

$CLICKHOUSE_CLIENT -n --query "
OPTIMIZE TABLE data_r1 PARTITION 1 FINAL;
${SYNC_ALL}
"

echo "after_optimize_parts_in_partition_1"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = 'data_r1'
      AND active
      AND partition_id = '1'
"

echo "after_optimize_rows_preserved"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
      AND partition_id = '1'
"

###############################################################################
# Test 2: OPTIMIZE TABLE FINAL across all partitions
# Each partition may be assigned to different replicas; FINAL merge must
# work correctly across all of them.
###############################################################################
$CLICKHOUSE_CLIENT -n --query "
INSERT INTO data_r1 (key, value, part_key) VALUES (200, 2000, 2);
INSERT INTO data_r1 (key, value, part_key) VALUES (201, 2010, 2);
${SYNC_ALL}
"

echo "before_table_final_rows"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
"

$CLICKHOUSE_CLIENT -n --query "
OPTIMIZE TABLE data_r1 FINAL;
${SYNC_ALL}
"

echo "after_table_final_rows"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
"

###############################################################################
# Test 3: OPTIMIZE on unassigned partition with no parts — noop behavior
# optimize_throw_if_noop=1 should report CANNOT_ASSIGN_OPTIMIZE;
# optimize_throw_if_noop=0 should be silent.
###############################################################################
echo "optimize_unassigned_noop_error"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE data_r1 PARTITION 99999 SETTINGS optimize_throw_if_noop = 1" 2>&1 | grep -o 'CANNOT_ASSIGN_OPTIMIZE\|no parts to merge' | head -1

echo "optimize_unassigned_noop_silent"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE data_r1 PARTITION 99999 SETTINGS optimize_throw_if_noop = 0" && echo "silent_ok"

###############################################################################
# Test 4: rf>1 single log entry for OPTIMIZE
# When rf=2, OPTIMIZE creates a single log entry and both assigned replicas
# execute the merge. Verify by checking that parts are merged on both replicas.
###############################################################################
echo "rf2_single_log_entry"

# Add extra parts to partition 0
$CLICKHOUSE_CLIENT -n --query "
INSERT INTO data_r1 (key, value, part_key) VALUES (300, 3000, 0);
INSERT INTO data_r1 (key, value, part_key) VALUES (301, 3010, 0);
${SYNC_ALL}
"

# Find which replicas are assigned to partition 0
ASSIGNED_R0=$($CLICKHOUSE_CLIENT --query "
    SELECT arrayJoin(assigned_replicas)
    FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'data_r1' AND partition_id = '0'
    ORDER BY assigned_replicas
")

echo "partition_0_assigned_replicas"
echo "$ASSIGNED_R0"

$CLICKHOUSE_CLIENT -n --query "
OPTIMIZE TABLE data_r1 PARTITION 0 FINAL;
${SYNC_ALL}
"

# Both assigned replicas should have exactly 1 active part in partition 0
echo "merged_parts_on_assigned_replicas"
$CLICKHOUSE_CLIENT --query "
    SELECT table, count()
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
      AND partition_id = '0'
    GROUP BY table
    ORDER BY table
"

###############################################################################
# Cleanup
###############################################################################
$CLICKHOUSE_CLIENT -n --query "
DROP TABLE data_r1 SYNC;
DROP TABLE data_r2 SYNC;
DROP TABLE data_r3 SYNC;
"
