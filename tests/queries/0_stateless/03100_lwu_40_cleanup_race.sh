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

    SET enable_lightweight_update = 1;
    SET insert_keeper_fault_injection_probability = 0.0;

    INSERT INTO t_lwu_cleanup_1 VALUES (1, 'v1') (2, 'v2') (3, 'v3');
    SYSTEM SYNC REPLICA t_lwu_cleanup_1;
    SYSTEM STOP MERGES t_lwu_cleanup_1;

    UPDATE t_lwu_cleanup_1 SET v = 'u2' WHERE k = 2;
    SYSTEM SYNC REPLICA t_lwu_cleanup_2;

    ALTER TABLE t_lwu_cleanup_2 UPDATE v = v || '_updated' WHERE 1 SETTINGS mutations_sync = 1;
    SYSTEM SYNC REPLICA t_lwu_cleanup_1 PULL;
"

for _ in {0..50}; do
    res=`$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_cleanup_2' AND active AND startsWith(name, 'patch')"`
    if [[ $res == "0" ]]; then
        break
    fi
    sleep 1.0
done

$CLICKHOUSE_CLIENT --query "
    SET apply_patch_parts =  1;

    SELECT _part, * FROM t_lwu_cleanup_1 ORDER BY k;
    SELECT _part, * FROM t_lwu_cleanup_2 ORDER BY k;

    SELECT table, name FROM system.parts
    WHERE database = currentDatabase() AND table IN ('t_lwu_cleanup_1', 't_lwu_cleanup_2') AND active
    ORDER BY table, name;

    SYSTEM START MERGES t_lwu_cleanup_1;
"

wait_for_mutation "t_lwu_cleanup_1" "0000000000"

for _ in {0..50}; do
    res=`$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_cleanup_1' AND active AND startsWith(name, 'patch')"`
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
