#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-catalog, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_cleanup_1 SYNC;
    DROP TABLE IF EXISTS t_lwu_cleanup_2 SYNC;

    CREATE TABLE t_lwu_cleanup_1 (k UInt64, v String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_cleanup', '1')
    ORDER BY k
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        cleanup_delay_period = 1,
        max_cleanup_delay_period = 1,
        cleanup_delay_period_random_add = 0;

    CREATE TABLE t_lwu_cleanup_2 (k UInt64, v String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_cleanup', '2')
    ORDER BY k
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        cleanup_delay_period = 1,
        max_cleanup_delay_period = 1,
        cleanup_delay_period_random_add = 0;

    -- Stop cleanup on both replicas to prevent premature patch part removal.
    -- SYSTEM STOP MERGES does not stop the cleanup thread, which can race
    -- by cleaning up patch parts before the mutation incorporates them.
    -- Cleanup must stay stopped on both replicas until the mutation completes
    -- on both replicas. Starting cleanup on replica 2 too early would create
    -- a DROP_PART log entry for the patch part that causes a three-way
    -- deadlock in replica 1's replication queue: DROP_PART blocks GET_PART
    -- (same part), MUTATE_PART waits for GET_PART, DROP_PART waits for
    -- MUTATE_PART.
    SYSTEM STOP CLEANUP t_lwu_cleanup_1;
    SYSTEM STOP CLEANUP t_lwu_cleanup_2;

    SET enable_lightweight_update = 1;
    SET insert_keeper_fault_injection_probability = 0.0;

    INSERT INTO t_lwu_cleanup_1 VALUES (1, 'v1') (2, 'v2') (3, 'v3');
    SYSTEM SYNC REPLICA t_lwu_cleanup_1;
    SYSTEM STOP MERGES t_lwu_cleanup_1;
    -- Stop fetches on replica 1 to prevent it from fetching all_0_0_0_2
    -- from replica 2 before the first set of assertions.
    SYSTEM STOP FETCHES t_lwu_cleanup_1;

    UPDATE t_lwu_cleanup_1 SET v = 'u2' WHERE k = 2;
    SYSTEM SYNC REPLICA t_lwu_cleanup_2;

    ALTER TABLE t_lwu_cleanup_2 UPDATE v = v || '_updated' WHERE 1 SETTINGS mutations_sync = 1;
    SYSTEM SYNC REPLICA t_lwu_cleanup_1 PULL;
"

# Phase 1: Check state before mutation on replica 1.
# Cleanup is stopped on both replicas, so the patch part is still present
# on both replicas. On replica 2 it is obsolete (mutation incorporated it
# into all_0_0_0_2) but not yet cleaned up.
$CLICKHOUSE_CLIENT --query "
    SET apply_patch_parts =  1;

    SELECT _part, * FROM t_lwu_cleanup_1 ORDER BY k;
    SELECT _part, * FROM t_lwu_cleanup_2 ORDER BY k;

    SELECT table, name FROM system.parts
    WHERE database = currentDatabase() AND table IN ('t_lwu_cleanup_1', 't_lwu_cleanup_2') AND active
    ORDER BY table, name;

    SYSTEM START FETCHES t_lwu_cleanup_1;
    SYSTEM START MERGES t_lwu_cleanup_1;
"

wait_for_mutation "t_lwu_cleanup_1" "0000000000"

# Start cleanup on both replicas after the mutation has completed on both.
$CLICKHOUSE_CLIENT --query "
    SYSTEM START CLEANUP t_lwu_cleanup_1;
    SYSTEM START CLEANUP t_lwu_cleanup_2;
"

for _ in {0..50}; do
    res=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table IN ('t_lwu_cleanup_1', 't_lwu_cleanup_2') AND active AND startsWith(name, 'patch')")
    if [[ $res == "0" ]]; then
        break
    fi
    sleep 1.0
done

$CLICKHOUSE_CLIENT --query "
    SET apply_patch_parts =  0;

    SELECT _part, * FROM t_lwu_cleanup_1 ORDER BY k;
    SELECT _part, * FROM t_lwu_cleanup_2 ORDER BY k;

    SELECT table, name FROM system.parts
    WHERE database = currentDatabase() AND table IN ('t_lwu_cleanup_1', 't_lwu_cleanup_2') AND active
    ORDER BY table, name;

    DROP TABLE IF EXISTS t_lwu_cleanup_1 SYNC;
    DROP TABLE IF EXISTS t_lwu_cleanup_2 SYNC;
"
