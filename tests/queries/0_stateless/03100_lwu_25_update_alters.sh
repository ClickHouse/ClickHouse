#!/usr/bin/env bash
# Tags: no-shared-merge-tree, long, no-parallel

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

${CLICKHOUSE_CLIENT} -n --query "
    DROP TABLE IF EXISTS t_lightweight_mut_4 SYNC;
    SET enable_lightweight_update = 1;

    CREATE TABLE t_lightweight_mut_4 (id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lightweight_mut_4', '1')
    ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    SYSTEM STOP MERGES t_lightweight_mut_4;
    INSERT INTO t_lightweight_mut_4 VALUES (1, 1);

    ALTER TABLE t_lightweight_mut_4 MODIFY COLUMN v String SETTINGS alter_sync = 0;
"

wait_for_mutation_to_start "t_lightweight_mut_4" "0000000000"

${CLICKHOUSE_CLIENT} -n --query "
    SET enable_lightweight_update = 1;
    UPDATE t_lightweight_mut_4 SET v = 'x' WHERE 1;

    SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_patch_parts = 0;
    SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_patch_parts = 1;

    SYSTEM START MERGES t_lightweight_mut_4;
"

wait_for_mutation "t_lightweight_mut_4" "0000000000"

${CLICKHOUSE_CLIENT} -n --query "
    SET enable_lightweight_update = 1;
    SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_patch_parts = 0;
    SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_patch_parts = 1;

    SYSTEM STOP MERGES t_lightweight_mut_4;
    UPDATE t_lightweight_mut_4 SET v = '100' WHERE 1;

    ALTER TABLE t_lightweight_mut_4 MODIFY COLUMN v UInt64 SETTINGS alter_sync = 0;
"

wait_for_mutation_to_start "t_lightweight_mut_4" "0000000001"

${CLICKHOUSE_CLIENT} -n --query "
    SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_patch_parts = 0;
    SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_patch_parts = 1;

    SYSTEM START MERGES t_lightweight_mut_4;
"

wait_for_mutation "t_lightweight_mut_4" "0000000001"

${CLICKHOUSE_CLIENT} -n --query "
    SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_patch_parts = 0;
    SELECT id, v, toTypeName(v) FROM t_lightweight_mut_4 ORDER BY id SETTINGS apply_patch_parts = 1;

    DROP TABLE t_lightweight_mut_4 SYNC;
"
