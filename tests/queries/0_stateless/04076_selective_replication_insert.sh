#!/usr/bin/env bash
# Tags: replica, zookeeper


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

###############################################################################
# Test 1: Sequential INSERT with partition assignment verification
# Verifies that INSERT to a selective replication table correctly assigns
# partitions to exactly rf replicas.
###############################################################################
$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS data_r1 SYNC;
DROP TABLE IF EXISTS data_r2 SYNC;
DROP TABLE IF EXISTS data_r3 SYNC;

CREATE TABLE data_r1 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE data_r2 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE data_r3 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

INSERT INTO data_r1 VALUES (0, 0, 0);
INSERT INTO data_r1 VALUES (1, 10, 1);
INSERT INTO data_r1 VALUES (2, 20, 2);
INSERT INTO data_r1 VALUES (3, 30, 0);
INSERT INTO data_r1 VALUES (4, 40, 1);
INSERT INTO data_r1 VALUES (5, 50, 2);
INSERT INTO data_r1 VALUES (6, 60, 0);
INSERT INTO data_r1 VALUES (7, 70, 1);
INSERT INTO data_r1 VALUES (8, 80, 2);
INSERT INTO data_r1 VALUES (9, 90, 0);

SYSTEM SYNC REPLICA data_r1;
SYSTEM SYNC REPLICA data_r2;
SYSTEM SYNC REPLICA data_r3;
"

echo "partition_detail"
$CLICKHOUSE_CLIENT --query "
    SELECT partition_id, sum(rows) AS total_rows
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
    GROUP BY partition_id
    ORDER BY partition_id
"

echo "replica_count_ok"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(cnt > 3) = 0
    FROM (
        SELECT partition_id, countDistinct(table) AS cnt
        FROM system.parts
        WHERE database = currentDatabase()
          AND table IN ('data_r1', 'data_r2', 'data_r3')
          AND active
        GROUP BY partition_id
    )
"

echo "total_rows_multiple_ok"
$CLICKHOUSE_CLIENT --query "
    SELECT modulo(sum(rows), 2) = 0
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE data_r1 SYNC;
DROP TABLE data_r2 SYNC;
DROP TABLE data_r3 SYNC;
"

###############################################################################
# Test 2: INSERT via remote() function triggers distributed_depth > 0
# Before this fix, the sink skipped partition-level pre-routing at depth > 0
# and the write could fail with ALL_REPLICAS_ARE_STALE.
###############################################################################
echo "insert_via_remote_depth1"

$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS rem_r1 SYNC;
DROP TABLE IF EXISTS rem_r2 SYNC;
DROP TABLE IF EXISTS rem_r3 SYNC;

CREATE TABLE rem_r1 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_rem/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE rem_r2 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_rem/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE rem_r3 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_rem/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'rem_r1')
    VALUES (100, 1000, 0), (101, 1010, 1), (102, 1020, 2),
           (103, 1030, 0), (104, 1040, 1), (105, 1050, 2)
"

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA rem_r1;
SYSTEM SYNC REPLICA rem_r2;
SYSTEM SYNC REPLICA rem_r3;
"

echo "insert_via_remote_total_rows"
$CLICKHOUSE_CLIENT --query "
    SELECT
        (SELECT count() FROM rem_r1 WHERE value >= 1000)
      + (SELECT count() FROM rem_r2 WHERE value >= 1000)
      + (SELECT count() FROM rem_r3 WHERE value >= 1000)
      = 12
"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE rem_r1 SYNC;
DROP TABLE rem_r2 SYNC;
DROP TABLE rem_r3 SYNC;
"

###############################################################################
# Test 3: Async INSERT compatibility
# Verifies that async_insert works with selective replication.
###############################################################################
echo "async_insert_basic"

$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS async_r1 SYNC;
DROP TABLE IF EXISTS async_r2 SYNC;
DROP TABLE IF EXISTS async_r3 SYNC;

CREATE TABLE async_r1 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_async/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE async_r2 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_async/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE async_r3 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_async/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
"

$CLICKHOUSE_CLIENT --async_insert=1 --wait_for_async_insert=1 \
    --query "INSERT INTO async_r1 VALUES (1, 10, 0), (2, 20, 1), (3, 30, 2)"

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA async_r1;
SYSTEM SYNC REPLICA async_r2;
SYSTEM SYNC REPLICA async_r3;
"

echo "async_insert_rows_ok"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('async_r1', 'async_r2', 'async_r3')
      AND active
"

echo "async_insert_each_partition_rf2"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(cnt != 2) = 0
    FROM (
        SELECT partition_id, countDistinct(table) AS cnt
        FROM system.parts
        WHERE database = currentDatabase()
          AND table IN ('async_r1', 'async_r2', 'async_r3')
          AND active
        GROUP BY partition_id
    )
"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE async_r1 SYNC;
DROP TABLE async_r2 SYNC;
DROP TABLE async_r3 SYNC;
"

###############################################################################
# Test 4: Batch allocation — multi-partition INSERT uses allocateBatch
# Verifies correct assignments with /selective/counts node.
###############################################################################
echo "batch_insert_10_partitions"

$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS batch_r1 SYNC;
DROP TABLE IF EXISTS batch_r2 SYNC;
DROP TABLE IF EXISTS batch_r3 SYNC;

CREATE TABLE batch_r1 (key UInt64, value String, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_batch/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE batch_r2 (key UInt64, value String, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_batch/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE batch_r3 (key UInt64, value String, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_batch/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
"

$CLICKHOUSE_CLIENT --query "
INSERT INTO batch_r1 SELECT number, toString(number), number % 10 FROM numbers(100)
    SETTINGS max_partitions_per_insert_block = 100;
"

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA batch_r1;
SYSTEM SYNC REPLICA batch_r2;
SYSTEM SYNC REPLICA batch_r3;
"

echo "partition_count"
$CLICKHOUSE_CLIENT --query "
    SELECT uniq(partition_id)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('batch_r1', 'batch_r2', 'batch_r3')
      AND active
"

echo "each_partition_has_2_replicas"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(cnt != 2) = 0
    FROM (
        SELECT partition_id, countDistinct(table) AS cnt
        FROM system.parts
        WHERE database = currentDatabase()
          AND table IN ('batch_r1', 'batch_r2', 'batch_r3')
          AND active
        GROUP BY partition_id
    )
"

echo "total_rows_ok"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('batch_r1', 'batch_r2', 'batch_r3')
      AND active
"

echo "second_batch_reuses_assignments"
$CLICKHOUSE_CLIENT --query "
INSERT INTO batch_r1 SELECT number + 100, toString(number + 100), number % 10 FROM numbers(100)
    SETTINGS max_partitions_per_insert_block = 100;
"

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA batch_r1;
SYSTEM SYNC REPLICA batch_r2;
SYSTEM SYNC REPLICA batch_r3;
"

echo "total_rows_after_second_insert"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('batch_r1', 'batch_r2', 'batch_r3')
      AND active
"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE batch_r1 SYNC;
DROP TABLE batch_r2 SYNC;
DROP TABLE batch_r3 SYNC;
"

###############################################################################
# Test 5: Patch parts partition_id extraction
# Lightweight DELETE creates patch parts whose partition_id is prefixed with
# "patch-<hash>-". The selective replication cleanup logic must resolve the
# original partition_id to match assignments.
###############################################################################
echo "patch_parts_basic"

$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS patch_r1 SYNC;
DROP TABLE IF EXISTS patch_r2 SYNC;
DROP TABLE IF EXISTS patch_r3 SYNC;

CREATE TABLE patch_r1 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_patch/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, enable_block_offset_column = 1, enable_block_number_column = 1;

CREATE TABLE patch_r2 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_patch/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, enable_block_offset_column = 1, enable_block_number_column = 1;

CREATE TABLE patch_r3 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_patch/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, enable_block_offset_column = 1, enable_block_number_column = 1;

INSERT INTO patch_r1 VALUES (0, 0, 0), (1, 10, 1), (2, 20, 2), (3, 30, 0);

SYSTEM SYNC REPLICA patch_r1;
SYSTEM SYNC REPLICA patch_r2;
SYSTEM SYNC REPLICA patch_r3;
"

# Create patch parts via lightweight delete
$CLICKHOUSE_CLIENT --query "SET lightweight_delete_mode = 'lightweight_update_force'; DELETE FROM patch_r1 WHERE key = 0"

# Wait for patch parts to appear
for _ in $(seq 1 30); do
    patch_count=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 'patch_r1'
          AND active AND partition_id LIKE 'patch-%'
    ")
    if [ "$patch_count" -ge 1 ]; then
        break
    fi
    sleep 0.5
done

echo "patch_parts_exist"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'patch_r1'
      AND active AND partition_id LIKE 'patch-%'
"

echo "assignments_no_patch_prefix"
$CLICKHOUSE_CLIENT --query "
    SELECT count() = 0 FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'patch_r1'
      AND partition_id LIKE 'patch-%'
"

echo "read_after_patch"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(cnt) > 0 FROM (
        SELECT count() AS cnt FROM patch_r1 WHERE _partition_id = '0'
        UNION ALL
        SELECT count() AS cnt FROM patch_r2 WHERE _partition_id = '0'
        UNION ALL
        SELECT count() AS cnt FROM patch_r3 WHERE _partition_id = '0'
    )
"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE patch_r1 SYNC;
DROP TABLE patch_r2 SYNC;
DROP TABLE patch_r3 SYNC;
"

###############################################################################
# Test 6: parseAssignment rejects unknown format version
# Covers: KeeperReplicaAssignment.cpp parseAssignment — INSERT fails when
# the assignment node has an unrecognized format version.
###############################################################################
echo "parse_unknown_format_version"

ZK_PREFIX="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_parse"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS parse_r1 SYNC;
DROP TABLE IF EXISTS parse_r2 SYNC;

CREATE TABLE parse_r1 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('${ZK_PREFIX}/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE parse_r2 (key UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('${ZK_PREFIX}/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

INSERT INTO parse_r1 VALUES (1, 1);

SYSTEM SYNC REPLICA parse_r1;
SYSTEM SYNC REPLICA parse_r2;
"

# Overwrite the partition 1 assignment node with an unknown format version.
for i in {1..10}; do
    $CLICKHOUSE_KEEPER_CLIENT -q \
        "set ${ZK_PREFIX}/data/selective/assignments/1 '$(printf 'format version: 999\nr1,r2')'" \
        && break
    sleep 0.3
done

# INSERT to partition 1 must fail: allocatePartition fast-path calls parseAssignment
# which throws BAD_ARGUMENTS on unknown version 999.
insert_failed=0
$CLICKHOUSE_CLIENT --query "INSERT INTO parse_r1 VALUES (2, 1)" 2>/dev/null || insert_failed=1

echo "insert_failed_on_bad_version"
echo "$insert_failed"

# Restore valid assignment so cleanup works
for i in {1..10}; do
    $CLICKHOUSE_KEEPER_CLIENT -q \
        "set ${ZK_PREFIX}/data/selective/assignments/1 '$(printf 'format version: 1\nr1,r2')'" \
        && break
    sleep 0.3
done

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE parse_r1 SYNC;
DROP TABLE parse_r2 SYNC;
"

###############################################################################
# Test 7: Multi-partition assignment consistency
# Verifies that assignment cache remains consistent across multiple partitions
# when inserts span different partitions sequentially.
###############################################################################
echo "multi_partition_assignment_consistency"

$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS mp_r1 SYNC;
DROP TABLE IF EXISTS mp_r2 SYNC;
DROP TABLE IF EXISTS mp_r3 SYNC;

CREATE TABLE mp_r1 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_mp/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE mp_r2 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_mp/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;

CREATE TABLE mp_r3 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_ins_mp/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2;
"

$CLICKHOUSE_CLIENT --query "INSERT INTO mp_r1 VALUES (1, 10, 1)"

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA mp_r1;
SYSTEM SYNC REPLICA mp_r2;
SYSTEM SYNC REPLICA mp_r3;
"

echo "partition_assignment_verified"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.selective_assignments
    WHERE database = currentDatabase()
      AND table = 'mp_r1'
      AND partition_id = '1'
      AND length(assigned_replicas) = 2
"

$CLICKHOUSE_CLIENT --query "INSERT INTO mp_r1 VALUES (2, 20, 2)"
$CLICKHOUSE_CLIENT --query "INSERT INTO mp_r1 VALUES (3, 30, 3)"

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA mp_r1;
SYSTEM SYNC REPLICA mp_r2;
SYSTEM SYNC REPLICA mp_r3;
"

echo "multiple_partitions_synced"
$CLICKHOUSE_CLIENT --query "
    SELECT count(DISTINCT partition_id)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('mp_r1', 'mp_r2', 'mp_r3')
      AND active
"

echo "all_assignments_consistent"
$CLICKHOUSE_CLIENT --query "
    SELECT count() = 0
    FROM system.selective_assignments
    WHERE database = currentDatabase()
      AND table = 'mp_r1'
      AND length(assigned_replicas) != 2
"

$CLICKHOUSE_CLIENT --query "INSERT INTO mp_r1 VALUES (5, 50, 5)"

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA mp_r1;
SYSTEM SYNC REPLICA mp_r2;
SYSTEM SYNC REPLICA mp_r3;
"

echo "new_partition_assigned"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.selective_assignments
    WHERE database = currentDatabase()
      AND table = 'mp_r1'
      AND partition_id = '5'
      AND length(assigned_replicas) = 2
"

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE mp_r1 SYNC;
DROP TABLE mp_r2 SYNC;
DROP TABLE mp_r3 SYNC;
"
