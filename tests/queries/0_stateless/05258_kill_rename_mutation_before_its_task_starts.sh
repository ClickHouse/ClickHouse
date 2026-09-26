#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# no-parallel: the failpoint is server-wide and pauses the first mutation task of any table.
# no-shared-merge-tree: the failpoint is in the mutation tasks of MergeTree and ReplicatedMergeTree tables.
# no-replicated-database: another replica of the database can mutate the replicated part before the kill.

# `KILL MUTATION` of a `RENAME COLUMN` whose task for a part was already selected, but has not
# started yet, must stop that task: the part keeps all its columns, whatever its type and storage.
# A live rename is not stopped when only an older patch of a part merged after it holds the old name.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="mt_mutate_task_pause_before_merge_list"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_no_free_threads" 2>/dev/null
}
trap cleanup EXIT

# $1 - table name, $2 - columns, $3 - the row, $4 - settings that choose the part type and storage,
# $5 - 1 to enable the block number and offset columns (default 0), $6 - statements to run before the RENAME
function kill_rename_before_its_task_starts()
{
    local table=$1 columns=$2 row=$3 settings=$4 block_columns=${5:-0} setup=${6:-}

    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $table ($columns) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS $settings, enable_block_number_column = $block_columns, enable_block_offset_column = $block_columns;
        INSERT INTO $table VALUES $row;
        $setup
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
            WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MutatePart' AND part_name LIKE 'all_1_1_0_%'")
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

# The part keeps the renamed column only as a missing-column marker.
kill_rename_before_its_task_starts t_compact_marker "v UInt32, x UInt32" "(0, 10)" \
    "$COMPACT, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns'"

# Only a patch part of a lightweight update holds the renamed column.
kill_rename_before_its_task_starts t_compact_patch "x UInt32" "(10)" "$COMPACT" 1 "
    ALTER TABLE t_compact_patch ADD COLUMN v UInt32;
    UPDATE t_compact_patch SET v = 5 WHERE 1 SETTINGS enable_lightweight_update = 1;"

# In ReplicatedMergeTree the entry fails once, and its retry clones the part without the killed rename.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_replicated (v UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', '1') ORDER BY tuple()
    SETTINGS $COMPACT, enable_block_number_column = 0, enable_block_offset_column = 0;
    INSERT INTO t_replicated SETTINGS insert_keeper_fault_injection_probability = 0 VALUES (1);
    SYSTEM ENABLE FAILPOINT $FP;
    ALTER TABLE t_replicated RENAME COLUMN v TO w SETTINGS mutations_sync = 0, alter_sync = 0;
"

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
$CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE database = currentDatabase() AND table = 't_replicated' FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"

deadline=$((SECONDS + 120))
while [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't_replicated'")" != "0" ]; do
    if [ $SECONDS -ge $deadline ]; then
        echo "The replication queue of t_replicated is not empty after 120 seconds"
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS part_log;

    SELECT table, part_type, part_storage_type FROM system.parts
    WHERE database = currentDatabase() AND active AND NOT startsWith(name, 'patch-') ORDER BY table;

    SELECT table, errorCodeToName(error) FROM system.part_log
    WHERE database = currentDatabase() AND event_type = 'MutatePart' AND part_name LIKE 'all_1_1_0_%' ORDER BY table;

    SELECT table, name, arraySort(groupArray(column)) FROM system.parts_columns
    WHERE database = currentDatabase() AND active AND NOT startsWith(name, 'patch-') GROUP BY table, name ORDER BY table;

    SELECT count() FROM system.mutations WHERE database = currentDatabase();

    SELECT table, count() FROM system.parts
    WHERE database = currentDatabase() AND active AND startsWith(name, 'patch-') GROUP BY table;

    SELECT part_name, errorCodeToName(error) FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_replicated' AND event_type = 'MutatePart' ORDER BY event_time_microseconds;

    SELECT count() FROM system.replication_queue WHERE database = currentDatabase();
"

# A live rename is not stopped: its part was merged after the rename and only an older patch of it holds the old name.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_live (x UInt32, v UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_live', '1') ORDER BY tuple()
    SETTINGS $COMPACT, enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0,
        merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;
    INSERT INTO t_live SETTINGS insert_keeper_fault_injection_probability = 0 VALUES (1, 1);
    UPDATE t_live SET v = 5 WHERE 1 SETTINGS enable_lightweight_update = 1, insert_keeper_fault_injection_probability = 0;
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
    ALTER TABLE t_live RENAME COLUMN v TO w SETTINGS alter_sync = 0, mutations_sync = 0;
    SYSTEM SYNC REPLICA t_live;
    OPTIMIZE TABLE t_live FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT name, has(groupArray(column), 'w'), has(groupArray(column), 'v') FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_live' AND active AND NOT startsWith(name, 'patch-') GROUP BY name;
    SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_live' AND active AND startsWith(name, 'patch-') AND column = 'v';

    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
"

deadline=$((SECONDS + 120))
while [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_live' AND NOT is_done")" != "0" ]; do
    if [ $SECONDS -ge $deadline ]; then
        echo "The mutation of t_live is not done after 120 seconds"
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS part_log;
    SELECT is_done, latest_fail_reason = '' FROM system.mutations WHERE database = currentDatabase() AND table = 't_live';
    SELECT countIf(error != 0) FROM system.part_log WHERE database = currentDatabase() AND table = 't_live' AND event_type = 'MutatePart';
"
