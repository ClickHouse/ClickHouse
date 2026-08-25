#!/usr/bin/env bash
# Tags: no-shared-catalog
# no-shared-catalog: STOP MERGES will only stop them only on one replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

function wait_for_mutation_to_start()
{
    local table=$1
    local mutation_id=$2

    for i in {1..300}
    do
        sleep 0.1
        if [[ $(${CLICKHOUSE_CLIENT} --query="SELECT parts_to_do FROM system.mutations WHERE database='$CLICKHOUSE_DATABASE' AND table='$table' AND mutation_id='$mutation_id'") -eq 1 ]]; then
            break
        fi

        if [[ $i -eq 300 ]]; then
            echo "Timed out while waiting for mutation to start."
        fi
    done
}

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_renames SYNC;
    SET enable_lightweight_update = 1;

    CREATE TABLE t_lwu_renames (a UInt64, b UInt64)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_on_fly/', '1')
    ORDER BY a
    SETTINGS
        min_bytes_for_wide_part = 0,
        enable_block_number_column = 1,
        enable_block_offset_column = 1;

    INSERT INTO t_lwu_renames SELECT number, number FROM numbers(10000);

    SELECT sum(b) FROM t_lwu_renames;

    UPDATE t_lwu_renames SET b = 0 WHERE a >= 100 AND a < 200;
    UPDATE t_lwu_renames SET b = 0 WHERE a >= 150 AND a < 250;

    SELECT sum(b) FROM t_lwu_renames SETTINGS apply_patch_parts = 0;
    SELECT sum(b) FROM t_lwu_renames SETTINGS apply_patch_parts = 1;

    SET alter_sync = 0;
    SET mutations_sync = 0;
    SYSTEM STOP MERGES t_lwu_renames;

    ALTER TABLE t_lwu_renames RENAME COLUMN b TO c;
"

function wait_for_rename()
{
    for i in {1..100}; do
        sleep 0.3
        ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE t_lwu_renames" | grep -q "\`c\` UInt64" && break;

        if [[ $i -eq 100 ]]; then
            echo "Timed out while waiting for rename to execute"
        fi
    done
}

wait_for_rename
wait_for_mutation_to_start "t_lwu_renames" "0000000000"

$CLICKHOUSE_CLIENT --query "
    SELECT sum(c) FROM t_lwu_renames SETTINGS apply_patch_parts = 0;
    SELECT sum(c) FROM t_lwu_renames SETTINGS apply_patch_parts = 1;
    SYSTEM START MERGES t_lwu_renames;
"

wait_for_mutation "t_lwu_renames" "0000000000"

$CLICKHOUSE_CLIENT --query "
    SELECT sum(c) FROM t_lwu_renames SETTINGS apply_patch_parts = 0;
    DROP TABLE t_lwu_renames SYNC;
"
