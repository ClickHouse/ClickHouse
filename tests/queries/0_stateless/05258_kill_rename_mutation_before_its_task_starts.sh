#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel: the failpoint is server-wide and pauses the first mutation task of any table.
# no-shared-merge-tree: the failpoint is in the mutation task of a non-replicated MergeTree table.

# `KILL MUTATION` of a `RENAME COLUMN` whose task for a part was already selected, but has not
# started yet, must stop that task: the part keeps all its columns, whatever its type and storage.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="mt_mutate_task_pause_before_merge_list"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

# $1 - table name, $2 - columns, $3 - the row, $4 - settings that choose the part type and storage
function kill_rename_before_its_task_starts()
{
    local table=$1 columns=$2 row=$3 settings=$4

    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $table ($columns) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS $settings, enable_block_number_column = 0, enable_block_offset_column = 0;
        INSERT INTO $table VALUES $row;
        SYSTEM ENABLE FAILPOINT $FP;
        ALTER TABLE $table RENAME COLUMN v TO w SETTINGS mutations_sync = 0, alter_sync = 0;
    "

    # The task of the RENAME for the part is selected and paused before it starts.
    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
    $CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database = currentDatabase() AND table = '$table' FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"

    for _ in {1..60}; do
        res=$($CLICKHOUSE_CLIENT --query "
            SYSTEM FLUSH LOGS part_log;
            SELECT count() FROM system.part_log
            WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MutatePart' AND part_name = 'all_1_1_0_2'")
        [ "$res" = "1" ] && break
        sleep 0.5
    done
}

COMPACT="min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0"
PACKED="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000"
WIDE="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0"

kill_rename_before_its_task_starts t_compact "v UInt32" "(1)" "$COMPACT"
kill_rename_before_its_task_starts t_compact_two_columns "v UInt32, x UInt32" "(1, 10)" "$COMPACT"
kill_rename_before_its_task_starts t_packed "v UInt32" "(1)" "$PACKED"
kill_rename_before_its_task_starts t_wide "v UInt32" "(1)" "$WIDE"

$CLICKHOUSE_CLIENT --query "
    SELECT table, part_type, part_storage_type FROM system.parts
    WHERE database = currentDatabase() AND active ORDER BY table;

    SELECT table, errorCodeToName(error) FROM system.part_log
    WHERE database = currentDatabase() AND event_type = 'MutatePart' AND part_name = 'all_1_1_0_2' ORDER BY table;

    SELECT table, name, arraySort(groupArray(column)) FROM system.parts_columns
    WHERE database = currentDatabase() AND active GROUP BY table, name ORDER BY table;

    SELECT count() FROM system.mutations WHERE database = currentDatabase();
"
