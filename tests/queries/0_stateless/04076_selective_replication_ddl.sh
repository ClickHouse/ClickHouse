#!/usr/bin/env bash
# Tags: replica, zookeeper


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

###############################################################################
# Test 1: ALTER DELETE
###############################################################################
echo "alter_delete"

$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS data_r1 SYNC;
DROP TABLE IF EXISTS data_r2 SYNC;
DROP TABLE IF EXISTS data_r3 SYNC;

CREATE TABLE data_r1 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;

CREATE TABLE data_r2 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;

CREATE TABLE data_r3 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;

INSERT INTO data_r1 SELECT number, number * 10, number % 5 FROM numbers(10);
SYSTEM SYNC REPLICA data_r1;
SYSTEM SYNC REPLICA data_r2;
SYSTEM SYNC REPLICA data_r3;

ALTER TABLE data_r1 DELETE WHERE key = 0 SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA data_r1;
SYSTEM SYNC REPLICA data_r2;
SYSTEM SYNC REPLICA data_r3;
"

echo "after_delete_total"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
"

echo "key_zero_gone"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(cnt) FROM (
        SELECT count() AS cnt FROM data_r1 WHERE key = 0
        UNION ALL
        SELECT count() AS cnt FROM data_r2 WHERE key = 0
        UNION ALL
        SELECT count() AS cnt FROM data_r3 WHERE key = 0
    )
"

###############################################################################
# Test 2: ALTER UPDATE
###############################################################################
echo "alter_update"

$CLICKHOUSE_CLIENT -n --query "
ALTER TABLE data_r1 UPDATE value = value * 100 WHERE 1 SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA data_r1;
SYSTEM SYNC REPLICA data_r2;
SYSTEM SYNC REPLICA data_r3;
"

echo "update_no_old_values"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(cnt) FROM (
        SELECT count() AS cnt FROM data_r1 WHERE value < 100
        UNION ALL
        SELECT count() AS cnt FROM data_r2 WHERE value < 100
        UNION ALL
        SELECT count() AS cnt FROM data_r3 WHERE value < 100
    )
"

echo "update_total_unchanged"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
"

###############################################################################
# Test 3: ALTER ADD COLUMN
# DDL propagation to all replicas is verified — ALTER ADD COLUMN must
# propagate to ALL 3 replicas, not just the assigned ones.
###############################################################################
echo "alter_add_column"

$CLICKHOUSE_CLIENT -n --query "
ALTER TABLE data_r1 ADD COLUMN value_str String DEFAULT 'hello' SETTINGS alter_sync = 2;
"

echo "ddl_propagated_to_all"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.columns
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND name = 'value_str'
"

###############################################################################
# Test 4: DROP PARTITION
###############################################################################
echo "drop_partition"

$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

ALTER TABLE data_r1 DROP PARTITION 0;
SYSTEM SYNC REPLICA data_r1;
SYSTEM SYNC REPLICA data_r2;
SYSTEM SYNC REPLICA data_r3;
"

echo "partition_0_gone"
$CLICKHOUSE_CLIENT --query "
SELECT count()
FROM system.parts
WHERE database = currentDatabase()
  AND table IN ('data_r1', 'data_r2', 'data_r3')
  AND active
  AND partition_id = '0'
"

###############################################################################
# Test 5: REPLACE PARTITION FROM source table
# Run REPLACE PARTITION from an assigned replica for partition 0 to avoid
# the single-server artifact where the issuing replica always writes locally.
###############################################################################
echo "replace_partition"

# Find one replica that is assigned to partition 0.
ASSIGNED_FOR_0=$($CLICKHOUSE_CLIENT --query "
    SELECT r
    FROM (
        SELECT arrayJoin(assigned_replicas) AS r
        FROM system.selective_assignments
        WHERE database = currentDatabase()
          AND table = 'data_r1'
          AND partition_id = '0'
    )
    LIMIT 1
")

if [ -z "${ASSIGNED_FOR_0}" ]; then
    ASSIGNED_TABLE="data_r1"
else
    ASSIGNED_TABLE="data_${ASSIGNED_FOR_0}"
fi

$CLICKHOUSE_CLIENT -n --send_logs_level=error --query "
DROP TABLE IF EXISTS data_src SYNC;

CREATE TABLE data_src (key UInt64, value UInt64, part_key UInt64, value_str String DEFAULT 'hello')
    ENGINE = MergeTree()
    PARTITION BY part_key ORDER BY key;

INSERT INTO data_src SELECT number, number * 100, 0, 'hello' FROM numbers(5);

ALTER TABLE ${ASSIGNED_TABLE} REPLACE PARTITION 0 FROM data_src;
SYSTEM SYNC REPLICA data_r1;
SYSTEM SYNC REPLICA data_r2;
SYSTEM SYNC REPLICA data_r3;
"

echo "replace_partition_0_row_count"
$CLICKHOUSE_CLIENT --query "
SELECT sum(rows)
FROM system.parts
WHERE database = currentDatabase()
  AND table IN ('data_r1', 'data_r2', 'data_r3')
  AND active
  AND partition_id = '0'
"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS data_src SYNC"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE data_r1 SYNC;
DROP TABLE data_r2 SYNC;
DROP TABLE data_r3 SYNC;
"

###############################################################################
# Test 6: ALTER replication_factor is forbidden (readonly setting)
###############################################################################
echo "constraint_alter_rf_forbidden"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;
DROP TABLE IF EXISTS c_r2 SYNC;

CREATE TABLE c_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c2/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE c_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c2/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
"

$CLICKHOUSE_CLIENT --query "
ALTER TABLE c_r1 MODIFY SETTING replication_factor = 1
" 2>/dev/null && echo "UNEXPECTED_OK" || echo "EXPECTED_ERROR"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE c_r1 SYNC;
DROP TABLE c_r2 SYNC;
"

###############################################################################
# Test 7: Mismatched replication_factor across replicas
###############################################################################
echo "constraint_rf_mismatch"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;
DROP TABLE IF EXISTS c_r2 SYNC;

CREATE TABLE c_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c3/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE c_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c3/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 1;
" 2>/dev/null && echo "UNEXPECTED_OK" || echo "EXPECTED_ERROR"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;
DROP TABLE IF EXISTS c_r2 SYNC;
"

###############################################################################
# Test 8: rf>0 replica joining rf=0 table
###############################################################################
echo "constraint_rf_nonzero_joining_zero"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;
DROP TABLE IF EXISTS c_r2 SYNC;

CREATE TABLE c_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c3b/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 0;

CREATE TABLE c_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c3b/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
" 2>/dev/null && echo "UNEXPECTED_OK" || echo "EXPECTED_ERROR"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;
DROP TABLE IF EXISTS c_r2 SYNC;
"

###############################################################################
# Test 9: select_sequential_consistency incompatible with replication_factor
###############################################################################
echo "test_sequential_consistency_incompatible"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;

CREATE TABLE c_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c4/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 1;
"

$CLICKHOUSE_CLIENT --query "
SELECT count() FROM c_r1
SETTINGS select_sequential_consistency = 1
" 2>&1 | grep -m1 -c "NOT_IMPLEMENTED\|not supported with selective replication"

$CLICKHOUSE_CLIENT --query "DROP TABLE c_r1 SYNC;"

###############################################################################
# Test 10: insert_quorum > replication_factor rejected
###############################################################################
echo "test_quorum_exceeds_replication_factor"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;
DROP TABLE IF EXISTS c_r2 SYNC;

CREATE TABLE c_r1 (key UInt64, part_key UInt64, val UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c5/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE c_r2 (key UInt64, part_key UInt64, val UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c5/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
"

$CLICKHOUSE_CLIENT --insert_quorum=3 --query "
INSERT INTO c_r1 VALUES (99, 999, 9)
" 2>&1 | grep -m1 -c "TOO_FEW_LIVE_REPLICAS\|insert_quorum.*cannot be greater"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;
DROP TABLE IF EXISTS c_r2 SYNC;
"

###############################################################################
# Test 11: replication_factor mismatch detected on ATTACH
# Simulate by tampering the ZK selective/config node after table creation,
# then DETACH + ATTACH to trigger the startup validation.
###############################################################################
echo "constraint_rf_mismatch_on_attach"

ZK_PREFIX="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_c6"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS c_r1 SYNC;

CREATE TABLE c_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('${ZK_PREFIX}/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
"

# Overwrite selective/config in ZK to simulate a different rf value (rf=3)
for i in {1..10}; do
    $CLICKHOUSE_KEEPER_CLIENT -q "set ${ZK_PREFIX}/data/selective/config '{\"replication_factor\":3}'" && break
    sleep 0.5
done

# Verify the ZK node was actually updated
$CLICKHOUSE_KEEPER_CLIENT -q "get ${ZK_PREFIX}/data/selective/config" | grep -q '"replication_factor":3' \
    || { echo "Failed to set ZK node for replication_factor mismatch test"; exit 1; }

# DETACH then ATTACH triggers the startup rf validation.
# The mismatch is detected asynchronously in the restarting thread, so it
# does not surface as a client error. Instead, the replica stays readonly.
$CLICKHOUSE_CLIENT --query "DETACH TABLE c_r1 SYNC;"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE c_r1;"

# After ATTACH the replica should be in readonly mode because of the mismatch.
$CLICKHOUSE_CLIENT --query "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 'c_r1'"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS c_r1 SYNC;"

###############################################################################
# Test 12: replication_factor degradation (rf > replica count)
# rf=5 exceeds 3 available replicas — degrades to effective rf=3.
# All 3 replicas must hold the partition data.
###############################################################################
echo "rf_degradation"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS deg_r1 SYNC;
DROP TABLE IF EXISTS deg_r2 SYNC;
DROP TABLE IF EXISTS deg_r3 SYNC;

CREATE TABLE deg_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_deg/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 5;

CREATE TABLE deg_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_deg/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 5;

CREATE TABLE deg_r3 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl_deg/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 5;
"

# INSERT with send_logs_level=fatal to suppress the expected degradation warning.
$CLICKHOUSE_CLIENT -n --query "SET send_logs_level='fatal'; INSERT INTO deg_r1 VALUES (1, 1);"

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA deg_r1;
SYSTEM SYNC REPLICA deg_r2;
SYSTEM SYNC REPLICA deg_r3;
"

echo "all_replicas_hold_data_after_degradation"
$CLICKHOUSE_CLIENT --query "
    SELECT countDistinct(table) = 3
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('deg_r1', 'deg_r2', 'deg_r3')
      AND active AND partition_id = '1'
"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE deg_r1 SYNC;
DROP TABLE deg_r2 SYNC;
DROP TABLE deg_r3 SYNC;
"

###############################################################################
# Test: ALTER TABLE MODIFY SETTING replication_factor is rejected (readonly)
###############################################################################
echo "modify_replication_factor_rejected"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS readonly_rf SYNC;

CREATE TABLE readonly_rf (key UInt64, value UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ddl/readonly_rf', 'r1')
    ORDER BY key
    SETTINGS replication_factor = 2;
"

# Attempt to modify replication_factor — must fail with READONLY_SETTING error
$CLICKHOUSE_CLIENT --query "
    ALTER TABLE readonly_rf MODIFY SETTING replication_factor = 3
" 2>/dev/null && echo "UNEXPECTED_OK" || echo "EXPECTED_ERROR"

$CLICKHOUSE_CLIENT --query "DROP TABLE readonly_rf SYNC;"
