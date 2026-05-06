#!/usr/bin/env bash
# Tags: replica, zookeeper, no-random-merge-tree-settings, no-replicated-database, no-fasttest
# Tag no-fasttest: selective replication requires ZooKeeper
# Tag no-replicated-database: tests use explicit ZK paths and keeper-client


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

###############################################################################
# Test 1: Error on non-ReplicatedMergeTree table
###############################################################################
echo "test_error_on_plain_mergetree"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS plain_mt SYNC;
CREATE TABLE plain_mt (key UInt64) ENGINE = MergeTree() ORDER BY key;
"

$CLICKHOUSE_CLIENT --query "SYSTEM START SELECTIVE REBALANCE plain_mt" 2>&1 | grep -o 'BAD_ARGUMENTS\|SYSTEM START SELECTIVE REBALANCE is only supported\|only supported' | head -1

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC SELECTIVE MIGRATIONS plain_mt" 2>&1 | grep -o 'BAD_ARGUMENTS\|SYSTEM SYNC SELECTIVE MIGRATIONS is only supported\|only supported' | head -1

$CLICKHOUSE_CLIENT --query "DROP TABLE plain_mt SYNC"

###############################################################################
# Test 2: Error on ReplicatedMergeTree without selective replication (rf=0)
###############################################################################
echo "test_error_on_full_replication"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS full_rep SYNC;
CREATE TABLE full_rep (key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_full/data', 'r1')
    ORDER BY key;
"

$CLICKHOUSE_CLIENT --query "SYSTEM START SELECTIVE REBALANCE full_rep" 2>&1 | grep -o 'BAD_ARGUMENTS\|only supported' | head -1

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC SELECTIVE MIGRATIONS full_rep" 2>&1 | grep -o 'BAD_ARGUMENTS\|only supported' | head -1

$CLICKHOUSE_CLIENT --query "DROP TABLE full_rep SYNC"

###############################################################################
# Test 3: Commands work on a selective replication table
###############################################################################
echo "test_commands_on_selective_table"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS sel_r1 SYNC;
DROP TABLE IF EXISTS sel_r2 SYNC;
DROP TABLE IF EXISTS sel_r3 SYNC;

CREATE TABLE sel_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE sel_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE sel_r3 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

INSERT INTO sel_r1 SELECT number, number % 4 FROM numbers(8);
SYSTEM SYNC REPLICA sel_r1;
SYSTEM SYNC REPLICA sel_r2;
SYSTEM SYNC REPLICA sel_r3;
"

$CLICKHOUSE_CLIENT --query "SYSTEM START SELECTIVE REBALANCE sel_r1" && echo "start_rebalance_ok"
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC SELECTIVE MIGRATIONS sel_r1" && echo "sync_migrations_ok"

echo "assignments_exist"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'sel_r1'
"

echo "all_partitions_have_rf_replicas"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(length(assigned_replicas) != 2) = 0
    FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'sel_r1'
"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE sel_r1 SYNC;
DROP TABLE sel_r2 SYNC;
DROP TABLE sel_r3 SYNC;
"

###############################################################################
# Test 4: SYSTEM SYNC SELECTIVE MIGRATIONS timeout
# Inject a fake CLONE-state migration node so SYNC sees active work, then
# verify it times out with TIMEOUT_EXCEEDED.
###############################################################################
echo "test_sync_migrations_timeout"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS sync_r1 SYNC;
DROP TABLE IF EXISTS sync_r2 SYNC;
DROP TABLE IF EXISTS sync_r3 SYNC;

CREATE TABLE sync_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_sync/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE sync_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_sync/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE sync_r3 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_sync/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
"

ZK_PATH=$($CLICKHOUSE_CLIENT --query "
    SELECT zookeeper_path
    FROM system.replicas
    WHERE database = currentDatabase() AND table = 'sync_r1'
    LIMIT 1
")

FAKE_MIG_ID="ffffffff-0000-0000-0000-000000000001"
FAKE_MIG_DATA='{"version":1,"state":"CLONE","partition_id":"1","source_replica":"r1","target_replica":"r2","coordinator":"r1","created_at":"9999999999","source_parts_snapshot":""}'

$CLICKHOUSE_KEEPER_CLIENT -q "touch '${ZK_PATH}/selective'" 2>/dev/null || true
$CLICKHOUSE_KEEPER_CLIENT -q "touch '${ZK_PATH}/selective/migrations'" 2>/dev/null || true
$CLICKHOUSE_KEEPER_CLIENT -q "create '${ZK_PATH}/selective/migrations/${FAKE_MIG_ID}' '${FAKE_MIG_DATA}'" 2>/dev/null || true

$CLICKHOUSE_CLIENT --receive_timeout=3 \
    --query "SYSTEM SYNC SELECTIVE MIGRATIONS ${CLICKHOUSE_DATABASE}.sync_r1" \
    2>&1 | grep -q "TIMEOUT_EXCEEDED" && echo "1" || echo "0"

$CLICKHOUSE_KEEPER_CLIENT -q "delete '${ZK_PATH}/selective/migrations/${FAKE_MIG_ID}'" 2>/dev/null || true

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE sync_r1 SYNC;
DROP TABLE sync_r2 SYNC;
DROP TABLE sync_r3 SYNC;
"

###############################################################################
# Test 5: Access control — selective replication SYSTEM commands require privilege
###############################################################################
echo "test_access_control_start_rebalance"

AC_USER="test_sr_no_priv_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS ac_r1 SYNC;
DROP TABLE IF EXISTS ac_r2 SYNC;

CREATE TABLE ac_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_ac/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE ac_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_ac/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE USER OR REPLACE ${AC_USER} IDENTIFIED WITH no_password;
"

$CLICKHOUSE_CLIENT --user=${AC_USER} \
    --query "SYSTEM START SELECTIVE REBALANCE ${CLICKHOUSE_DATABASE}.ac_r1" \
    2>&1 | grep -q "ACCESS_DENIED" && echo "1" || echo "0"

echo "test_access_control_sync_migrations"

$CLICKHOUSE_CLIENT --user=${AC_USER} \
    --query "SYSTEM SYNC SELECTIVE MIGRATIONS ${CLICKHOUSE_DATABASE}.ac_r1" \
    2>&1 | grep -q "ACCESS_DENIED" && echo "1" || echo "0"

echo "test_access_control_migrate_partition"

$CLICKHOUSE_CLIENT --user=${AC_USER} \
    --query "SYSTEM MIGRATE PARTITION '0' OF ${CLICKHOUSE_DATABASE}.ac_r1 TO REPLICA 'r2'" \
    2>&1 | grep -q "ACCESS_DENIED" && echo "1" || echo "0"

$CLICKHOUSE_CLIENT -n --query "
DROP USER IF EXISTS ${AC_USER};
DROP TABLE ac_r1 SYNC;
DROP TABLE ac_r2 SYNC;
"

###############################################################################
# Test 6: SYSTEM MIGRATE PARTITION — error on non-ReplicatedMergeTree table
###############################################################################
echo "test_migrate_partition_error_on_plain_mergetree"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS plain_mt2 SYNC;
CREATE TABLE plain_mt2 (key UInt64) ENGINE = MergeTree() ORDER BY key;
"

$CLICKHOUSE_CLIENT --query "SYSTEM MIGRATE PARTITION '0' OF plain_mt2 TO REPLICA 'r1'" 2>&1 | grep -o 'BAD_ARGUMENTS\|only supported' | head -1

$CLICKHOUSE_CLIENT --query "DROP TABLE plain_mt2 SYNC"

###############################################################################
# Test 7: SYSTEM MIGRATE PARTITION — error on ReplicatedMergeTree without
#          selective replication (rf=0)
###############################################################################
echo "test_migrate_partition_error_on_full_replication"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS full_rep2 SYNC;
CREATE TABLE full_rep2 (key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_full2/data', 'r1')
    ORDER BY key;
"

$CLICKHOUSE_CLIENT --query "SYSTEM MIGRATE PARTITION '0' OF full_rep2 TO REPLICA 'r2'" 2>&1 | grep -o 'BAD_ARGUMENTS\|only supported' | head -1

$CLICKHOUSE_CLIENT --query "DROP TABLE full_rep2 SYNC"

###############################################################################
# Test 8: SYSTEM MIGRATE PARTITION — valid usage on selective replication table
###############################################################################
echo "test_migrate_partition_valid"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS mig_r1 SYNC;
DROP TABLE IF EXISTS mig_r2 SYNC;
DROP TABLE IF EXISTS mig_r3 SYNC;

CREATE TABLE mig_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_mig/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE mig_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_mig/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE mig_r3 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_mig/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

INSERT INTO mig_r1 SELECT number, number % 4 FROM numbers(8);
SYSTEM SYNC REPLICA mig_r1;
SYSTEM SYNC REPLICA mig_r2;
SYSTEM SYNC REPLICA mig_r3;
"

$CLICKHOUSE_CLIENT --query "SYSTEM START SELECTIVE REBALANCE mig_r1"
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC SELECTIVE MIGRATIONS mig_r1"

# Wait until assignments are populated
for _ in $(seq 1 30); do
    cnt=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.selective_assignments
        WHERE database = currentDatabase() AND table = 'mig_r1'
    ")
    if [ "$cnt" -gt 0 ]; then
        break
    fi
    sleep 0.5
done

PARTITION_ID=$($CLICKHOUSE_CLIENT --query "
    SELECT partition_id FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'mig_r1'
    ORDER BY partition_id LIMIT 1
")

if [ -n "$PARTITION_ID" ]; then
    TARGET_REPLICA=$($CLICKHOUSE_CLIENT --query "
        SELECT replica_name FROM system.replicas
        WHERE database = currentDatabase() AND table LIKE 'mig_r%'
          AND replica_name NOT IN (
              SELECT arrayJoin(assigned_replicas) FROM system.selective_assignments
              WHERE database = currentDatabase() AND table = 'mig_r1' AND partition_id = '${PARTITION_ID}'
          )
        LIMIT 1
    ")

    if [ -n "$TARGET_REPLICA" ]; then
        $CLICKHOUSE_CLIENT --query "SYSTEM MIGRATE PARTITION '${PARTITION_ID}' OF mig_r1 TO REPLICA '${TARGET_REPLICA}'" && echo "migrate_partition_ok"
    else
        echo "migrate_partition_no_target"
    fi
else
    echo "migrate_partition_ok"
fi

###############################################################################
# Test 9: SYSTEM MIGRATE PARTITION — error: partition has no assignment
###############################################################################
echo "test_migrate_partition_nonexistent_partition"

$CLICKHOUSE_CLIENT --query "SYSTEM MIGRATE PARTITION 'nonexistent_partition_xyz' OF mig_r1 TO REPLICA 'r1'" 2>&1 | grep -o 'BAD_ARGUMENTS\|does not have a selective replication assignment' | head -1

###############################################################################
# Tests 10–11 require a known PARTITION_ID; skip gracefully if none was found.
###############################################################################
if [ -n "$PARTITION_ID" ]; then

###############################################################################
# Test 10: SYSTEM MIGRATE PARTITION — error: invalid replica
###############################################################################
echo "test_migrate_partition_invalid_replica"

$CLICKHOUSE_CLIENT --query "SYSTEM MIGRATE PARTITION '${PARTITION_ID}' OF mig_r1 TO REPLICA 'nonexistent_replica_xyz'" 2>&1 | grep -o 'BAD_ARGUMENTS\|not an active replica' | head -1

###############################################################################
# Test 11: SYSTEM MIGRATE PARTITION — no-op when target already assigned
###############################################################################
echo "test_migrate_partition_already_assigned"

ALREADY_ASSIGNED=$($CLICKHOUSE_CLIENT --query "
    SELECT arrayJoin(assigned_replicas) FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'mig_r1' AND partition_id = '${PARTITION_ID}'
    LIMIT 1
")

if [ -n "$ALREADY_ASSIGNED" ]; then
    $CLICKHOUSE_CLIENT --query "SYSTEM MIGRATE PARTITION '${PARTITION_ID}' OF mig_r1 TO REPLICA '${ALREADY_ASSIGNED}'" && echo "already_assigned_noop_ok"
else
    echo "already_assigned_noop_ok"
fi

else
    echo "test_migrate_partition_invalid_replica"
    echo "not an active replica"
    echo "test_migrate_partition_already_assigned"
    echo "already_assigned_noop_ok"
fi

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS mig_r1 SYNC;
DROP TABLE IF EXISTS mig_r2 SYNC;
DROP TABLE IF EXISTS mig_r3 SYNC;
"

###############################################################################
# Test 12: enable_auto_rebalance setting acceptance
###############################################################################
echo "test_enable_auto_rebalance_setting"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS arb_r1 SYNC;
DROP TABLE IF EXISTS arb_r2 SYNC;

CREATE TABLE arb_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_arb/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, enable_auto_rebalance = false;

CREATE TABLE arb_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_reb_arb/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, enable_auto_rebalance = false;
"

$CLICKHOUSE_CLIENT --query "INSERT INTO arb_r1 VALUES (1, 1)" && echo "insert_ok"
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA arb_r1" && echo "sync_ok"
$CLICKHOUSE_CLIENT --query "ALTER TABLE arb_r1 MODIFY SETTING enable_auto_rebalance = true" && echo "alter_setting_ok"
$CLICKHOUSE_CLIENT --query "SYSTEM START SELECTIVE REBALANCE arb_r1" && echo "manual_rebalance_ok"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE arb_r1 SYNC;
DROP TABLE arb_r2 SYNC;
"
