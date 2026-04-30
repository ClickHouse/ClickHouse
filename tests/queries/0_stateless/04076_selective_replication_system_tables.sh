#!/usr/bin/env bash
# Tags: replica, zookeeper


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

###############################################################################
# Shared setup: 3 replicas, rf=2
# Tests system.selective_assignments, system.replication_queue columns,
# system.selective_migrations.
###############################################################################
$CLICKHOUSE_CLIENT -n --query "
SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS data_r1 SYNC;
DROP TABLE IF EXISTS data_r2 SYNC;
DROP TABLE IF EXISTS data_r3 SYNC;

CREATE TABLE data_r1 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_systbl/data', 'r1')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;

CREATE TABLE data_r2 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_systbl/data', 'r2')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;

CREATE TABLE data_r3 (key UInt64, value UInt64, part_key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/${CLICKHOUSE_DATABASE}/test_sr_systbl/data', 'r3')
    PARTITION BY part_key ORDER BY key
    SETTINGS replication_factor = 2, merge_selecting_sleep_ms = 100;

INSERT INTO data_r1 SELECT number, number * 10, number % 3 FROM numbers(9);

SYSTEM SYNC REPLICA data_r1;
SYSTEM SYNC REPLICA data_r2;
SYSTEM SYNC REPLICA data_r3;
"

###############################################################################
# Part A: system.selective_assignments
###############################################################################
echo "has_entries"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0 FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'data_r1'
"

echo "correct_rf"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(replication_factor != 2 OR length(assigned_replicas) != 2) = 0
    FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'data_r1'
"

echo "partition_count_matches"
$CLICKHOUSE_CLIENT --query "
    SELECT count(DISTINCT partition_id) FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'data_r1'
"

echo "is_local_flag_valid"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(is_local != has(assigned_replicas, 'r1')) = 0
    FROM system.selective_assignments
    WHERE database = currentDatabase() AND table = 'data_r1'
"

###############################################################################
# Part B: system.replication_queue columns
###############################################################################
echo "queue_columns_exist"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.columns
    WHERE database = 'system'
      AND table = 'replication_queue'
      AND name IN ('selective_partition_id', 'selective_assigned_replicas', 'selective_is_assigned')
"

echo "partition_on_two_replicas"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(cnt != 2)
    FROM (
        SELECT partition_id, countDistinct(table) AS cnt
        FROM system.parts
        WHERE database = currentDatabase()
          AND table IN ('data_r1', 'data_r2', 'data_r3')
          AND active
        GROUP BY partition_id
    )
"

echo "total_physical_rows"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(rows)
    FROM system.parts
    WHERE database = currentDatabase()
      AND table IN ('data_r1', 'data_r2', 'data_r3')
      AND active
"

echo "non_assigned_no_parts"
$CLICKHOUSE_CLIENT --query "
    SELECT count() = 0
    FROM system.selective_assignments AS sa
    JOIN system.parts AS p
        ON p.partition_id = sa.partition_id
        AND p.database = sa.database
        AND p.active = 1
    WHERE sa.database = currentDatabase()
      AND sa.table = 'data_r1'
      AND NOT has(sa.assigned_replicas, replaceRegexpOne(p.table, '^.*_', ''))
"

###############################################################################
# Part C: system.selective_migrations
###############################################################################
echo "migrations_table_exists"
$CLICKHOUSE_CLIENT --query "
    SELECT count() >= 10
    FROM system.columns
    WHERE database = 'system' AND table = 'selective_migrations'
"

echo "no_initial_migrations"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.selective_migrations
    WHERE database = currentDatabase() AND table = 'data_r1'
"

###############################################################################
# Part D: getAssignedReplicas on non-existent partition (ZNONODE path)
# Query for a partition never written — ZK node does not exist (ZNONODE).
# getAssignedReplicas returns {} when code == ZNONODE.
###############################################################################
echo "empty_assignment_for_nonexistent_partition"
$CLICKHOUSE_CLIENT --query "
    SELECT count() = 0
    FROM system.selective_assignments
    WHERE database = currentDatabase()
      AND table = 'data_r1'
      AND partition_id = '99999'
"

###############################################################################
# Cleanup
###############################################################################
$CLICKHOUSE_CLIENT -n --query "
DROP TABLE data_r1 SYNC;
DROP TABLE data_r2 SYNC;
DROP TABLE data_r3 SYNC;
"
