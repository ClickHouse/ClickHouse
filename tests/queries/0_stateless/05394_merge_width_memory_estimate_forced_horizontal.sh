#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `merge_memory_estimate_per_source_part_column` lets a range merge wider than a horizontal merge affords
# when the range will merge vertically. These are the cases where the range still holds many columns of
# every source part at once:
#   - `OPTIMIZE ... DEDUPLICATE` always merges horizontally;
#   - columns added by `ALTER TABLE ... ADD COLUMN` after every source part was written are expired by the
#     merge instead of gathered, which can leave too few columns to merge vertically;
#   - an aggregate projection that every source part has is merged by a child merge after the parent one,
#     and that child merge is horizontal.
#
# The tables have 16 columns and a single key column, like in `05218_merge_width_memory_estimate_vertical`.
# The estimate is sized from the server's actual memory limit so that a horizontal merge of 16 columns
# affords exactly two parts, while a vertical merge of the same table affords 16 times as many. Background
# merges may take some of the parts before `OPTIMIZE` does, so the test checks what every merge did rather
# than how many parts are left.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

# A background merge may still be running when `OPTIMIZE` finds nothing left to take, and its `part_log`
# entry appears only when it finishes.
function wait_for_merges()
{
    local table=$1
    for _ in {1..600}
    do
        [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = '$table'")" = 0 ] && return
        sleep 0.1
    done
    echo "Merges of $table did not finish"
}

# Creates `$table` with the columns `$columns`, inserts eight parts of one row into the first `$num_columns`
# columns, runs `$before_optimize`, then `OPTIMIZE TABLE $table $optimize_modifier`, and prints every
# (algorithm, width) of the merges that ran. `$extra_settings` are added to the table settings.
function run()
{
    local table=$1
    local columns=$2
    local num_columns=$3
    local min_columns_to_activate=$4
    local before_optimize=$5
    local optimize_modifier=$6
    local extra_settings=$7

    local values
    values=$(for _ in $(seq 1 "$num_columns"); do echo -n "number, "; done)
    values=${values%, }

    local inserts=""
    for i in {0..7}
    do
        inserts+="INSERT INTO $table SELECT $values FROM numbers($i, 1);"
    done

    $CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table ($columns)
    ENGINE = MergeTree ORDER BY k
    SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
        min_parts_to_merge_at_once = 2,
        merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        min_rows_for_full_part_storage = 0,
        allow_vertical_merges_from_compact_to_wide_parts = 1,
        vertical_merge_algorithm_min_columns_to_activate = $min_columns_to_activate,
        enable_vertical_merge_algorithm = 1,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_bytes_to_activate = 0,
        deduplicate_merge_projection_mode = 'rebuild',
        enable_block_number_column = 0,
        enable_block_offset_column = 0
        $extra_settings;

    SYSTEM STOP MERGES $table;
    $inserts
    $before_optimize
    SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
    SYSTEM START MERGES $table;

    OPTIMIZE TABLE $table $optimize_modifier;
    "

    wait_for_merges "$table"

    $CLICKHOUSE_CLIENT --query "
    SELECT sum(k), count() FROM $table;

    SYSTEM FLUSH LOGS part_log;

    SELECT DISTINCT merge_algorithm, length(merged_from) FROM system.part_log
    WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts'
    ORDER BY ALL;

    DROP TABLE $table;
    "
}

COLUMNS_16="k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
    c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64"

# `OPTIMIZE ... DEDUPLICATE` merges horizontally, so it takes two parts. The background merges would
# merge vertically and take all eight parts before `OPTIMIZE` does, so the selector base is raised far
# above what eight fresh parts of the same size reach; `OPTIMIZE` selects with the base of one.
echo 'deduplicate'
run t_merge_width_deduplicate "$COLUMNS_16" 16 1 "" "DEDUPLICATE" ", merge_selector_algorithm = 'Simple', merge_selector_base = 1000"

# Eleven columns including the key, then five more added without a default: they are absent from every
# source part, so the merge expires them and gathers only ten columns, fewer than the eleven the vertical
# algorithm needs (`_block_number` and `_block_offset` would be gathered as well, so they are disabled).
# Every merge is horizontal and priced by all 16 columns, so it takes two parts.
echo 'added columns'
run t_merge_width_added_columns \
    "k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64, c10 UInt64" 11 11 \
    "ALTER TABLE t_merge_width_added_columns ADD COLUMN c11 UInt64, ADD COLUMN c12 UInt64, ADD COLUMN c13 UInt64, ADD COLUMN c14 UInt64, ADD COLUMN c15 UInt64;" ""

# The parent merges vertically, but the aggregate projection with 16 columns that every part has is merged
# by a horizontal child merge, so every merge takes two parts.
echo 'aggregate projection'
run t_merge_width_aggregate_projection \
    "$COLUMNS_16, PROJECTION p (SELECT k, sum(c1), sum(c2), sum(c3), sum(c4), sum(c5), sum(c6), sum(c7), sum(c8), sum(c9), sum(c10), sum(c11), sum(c12), sum(c13), sum(c14), sum(c15) GROUP BY k)" \
    16 1 "" ""
